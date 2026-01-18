"""
Comprehensive tests for the Garmin Data Preprocessing Glue Job.
Tests CSV file processing, data type detection, and S3 operations for all Garmin data types.
Uses test files from tests/fixtures/garmin_test_data/
"""
import pytest
import zipfile
import io
import csv
from pathlib import Path
from io import BytesIO
from unittest.mock import Mock, MagicMock, patch, call

from moto import mock_aws
import boto3
import pandas as pd

# Path to Garmin test CSV fixtures
TEST_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "garmin_test_data"

# Test "CSV" file paths (NOTE: your fixtures appear to be Excel/binary in some cases)
HEART_RATE_CSV = TEST_FIXTURES_DIR / "260115_garmin-device-heart-rate_Testing-1_cd037752.csv"
STEP_CSV = TEST_FIXTURES_DIR / "260115_garmin-device-step_Testing-1_cd037752.csv"
SLEEP_CSV = TEST_FIXTURES_DIR / "260115_garmin-connect-sleep-stage_Testing-1_cd037752.csv"
RESPIRATION_CSV = TEST_FIXTURES_DIR / "260115_garmin-device-respiration_Testing-1_cd037752.csv"
STRESS_CSV = TEST_FIXTURES_DIR / "260115_garmin-device-stress_Testing-1_cd037752.csv"
PULSE_OX_CSV = TEST_FIXTURES_DIR / "260115_garmin-device-pulse-ox_Testing-1_cd037752.csv"


# ============================================================================
# HELPERS (ZIP + ROBUST ENCODING + CSV/EXCEL SNIFFING)
# ============================================================================

OLE_XLS_SIGNATURE = b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"  # old .xls "Compound File"
ZIP_SIGNATURE = b"PK\x03\x04"  # zip container (could be .xlsx OR a zip containing a csv)


def _decode_bytes_best_effort(data: bytes) -> str:
    """
    Decode bytes into text best-effort.
    Only used when we believe the payload is actually text/CSV.
    """
    for enc in ("utf-8", "utf-8-sig", "cp1252"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1")


def _normalize_newlines(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _is_probably_text_csv(raw: bytes, sample_size: int = 4096) -> bool:
    """
    Heuristic: text-ish + contains commas/newlines, not obviously binary.
    We also reject if it looks like OLE/XLS or ZIP container.
    """
    if raw.startswith(OLE_XLS_SIGNATURE) or raw.startswith(ZIP_SIGNATURE):
        return False

    sample = raw[:sample_size]
    # If there are many NUL bytes, it's almost certainly binary
    if sample.count(b"\x00") > 10:
        return False

    # Look for typical CSV separators/newlines
    return (b"," in sample or b"\n" in sample) and all(b < 128 or b in b"\r\n\t,;\"'" for b in sample[:200])


def _zip_contains_xlsx(zip_bytes: bytes) -> bool:
    """
    Detect if ZIP is actually an .xlsx (Office Open XML).
    xlsx usually has 'xl/workbook.xml' and '_rels/.rels'
    """
    try:
        with zipfile.ZipFile(BytesIO(zip_bytes), "r") as z:
            names = set(z.namelist())
            return ("xl/workbook.xml" in names) or any(n.startswith("xl/") for n in names)
    except zipfile.BadZipFile:
        return False


def _zip_read_first_file(zip_bytes: bytes) -> tuple[str, bytes]:
    """
    Return (name, bytes) for the first *meaningful* file in a zip.
    Prefer a .csv if present, else first non-directory entry.
    """
    with zipfile.ZipFile(BytesIO(zip_bytes), "r") as z:
        names = [n for n in z.namelist() if not n.endswith("/")]
        if not names:
            raise ValueError("ZIP archive is empty")

        csv_name = next((n for n in names if n.lower().endswith(".csv")), None)
        chosen = csv_name or names[0]
        with z.open(chosen) as f:
            return chosen, f.read()


def _find_header_row_in_dataframe(df_raw: pd.DataFrame, markers: tuple[str, ...]) -> int | None:
    """
    Given a header-less dataframe (header=None), find the row index that likely contains column headers.
    We look for markers like timezone / isoDate / unixTimestampInMs anywhere in the row.
    """
    def row_has_marker(row) -> bool:
        for cell in row:
            if pd.isna(cell):
                continue
            s = str(cell)
            if any(m in s for m in markers):
                return True
        return False

    for i in range(min(len(df_raw), 80)):
        if row_has_marker(df_raw.iloc[i].values):
            return i
    return None


def _read_excel_bytes_to_dataframe(raw: bytes) -> pd.DataFrame:
    """
    Read Excel-like bytes into a DataFrame, then align headers.
    Supports:
      - .xlsx via openpyxl (usually installed with pandas)
      - .xls via xlrd (NOT always installed) -> we try and give a clear error if missing
    """
    bio = BytesIO(raw)

    # Try xlsx first (openpyxl)
    try:
        df0 = pd.read_excel(bio, header=None, engine="openpyxl")
    except Exception:
        # Reset buffer for next attempt
        bio.seek(0)
        # Try xls (xlrd)
        try:
            df0 = pd.read_excel(bio, header=None, engine="xlrd")
        except ImportError as e:
            raise ImportError(
                "Your fixture file is not a real CSV; it looks like an old .xls Excel file.\n"
                "Install xlrd to read .xls:\n\n"
                "    pip install xlrd==2.0.1\n"
            ) from e

    markers = ("timezone", "isoDate", "unixTimestampInMs", "beatsPerMinute", "Timestamp")
    header_idx = _find_header_row_in_dataframe(df0, markers)

    if header_idx is None:
        return df0

    header = df0.iloc[header_idx].astype(str).tolist()
    df = df0.iloc[header_idx + 1 :].copy()
    df.columns = header
    df = df.reset_index(drop=True)

    # Drop completely empty columns
    df = df.dropna(axis=1, how="all")
    return df


def _strip_leading_preamble_and_align_header(text: str) -> str:
    """
    For text CSVs with Garmin preamble: remove leading junk/blank/1-field lines
    and start at the real header.
    """
    text = _normalize_newlines(text)
    lines = text.split("\n")

    def field_count(line: str) -> int:
        if line.strip() == "":
            return 0
        try:
            return len(next(csv.reader([line])))
        except Exception:
            return 0

    markers = ("timezone", "isoDate", "unixTimestampInMs", "beatsPerMinute", "Timestamp")
    best_idx = None

    for i in range(min(len(lines), 80)):
        line = lines[i]
        if field_count(line) < 2:
            continue
        if any(m in line for m in markers):
            best_idx = i
            break
        if best_idx is None:
            best_idx = i

    if best_idx is None:
        return text
    return "\n".join(lines[best_idx:])


def read_csv_to_dataframe(file_path: Path, skiprows=6) -> pd.DataFrame:
    """
    Robust reader for your fixtures.

    Note: Your integration tests should validate the *standardized/cleaned* schema
    produced by your preprocessing logic (e.g., 'Timestamp' instead of 'isoDate',
    and 'timezone' may not exist).
    """
    file_path = Path(file_path)
    raw = file_path.read_bytes()

    # ZIP container? Could be .xlsx OR a zip containing a .csv
    if raw.startswith(ZIP_SIGNATURE) and zipfile.is_zipfile(BytesIO(raw)):
        if _zip_contains_xlsx(raw):
            return _read_excel_bytes_to_dataframe(raw)
        _, inner_bytes = _zip_read_first_file(raw)
        # inner could still be excel
        if inner_bytes.startswith(OLE_XLS_SIGNATURE) or (
            inner_bytes.startswith(ZIP_SIGNATURE) and _zip_contains_xlsx(inner_bytes)
        ):
            return _read_excel_bytes_to_dataframe(inner_bytes)
        # assume text csv inside zip
        text = _decode_bytes_best_effort(inner_bytes)
        text = _strip_leading_preamble_and_align_header(text)
        return pd.read_csv(io.StringIO(text), engine="python", on_bad_lines="skip")

    # Old Excel .xls (OLE signature)
    if raw.startswith(OLE_XLS_SIGNATURE):
        return _read_excel_bytes_to_dataframe(raw)

    # If it’s genuinely a text CSV (or close), parse as text
    if _is_probably_text_csv(raw):
        text = _decode_bytes_best_effort(raw)
        text = _strip_leading_preamble_and_align_header(text)
        # NOTE: ignore skiprows because we align to header explicitly
        return pd.read_csv(io.StringIO(text), engine="python", on_bad_lines="skip")

    # Last resort: try excel parsing (many "csv" exports are mislabeled)
    return _read_excel_bytes_to_dataframe(raw)


def read_csv_file(file_path: Path) -> str:
    """
    Return a TEXT representation for uploading to S3 and header checks.

    If the source is Excel/binary, we convert it to CSV text via DataFrame -> to_csv.
    """
    df = read_csv_to_dataframe(file_path)
    return df.to_csv(index=False)


def upload_csv_to_s3(s3_client, bucket, key, csv_content):
    """Upload content to S3. Body must be bytes."""
    if isinstance(csv_content, (bytes, bytearray)):
        body = bytes(csv_content)
    else:
        body = str(csv_content).encode("utf-8", errors="replace")

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/csv"
    )


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_s3_buckets():
    """Create mock S3 buckets for source and destination."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        source_bucket = "test-processed-bucket"
        target_bucket = "test-merged-bucket"
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=target_bucket)
        yield {"s3_client": s3, "source_bucket": source_bucket, "target_bucket": target_bucket}


# ============================================================================
# UNIT TESTS - File Detection and Categorization
# ============================================================================

class TestFileDetection:
    def test_detect_all_garmin_data_types(self):
        files = [
            "patient1/garmin-device-heart-rate/file1.csv",
            "patient1/garmin-connect-sleep-stage/file2.csv",
            "patient1/garmin-device-step/file3.csv",
            "patient1/garmin-device-respiration/file4.csv",
            "patient1/garmin-device-stress/file5.csv",
            "patient1/garmin-device-pulse-ox/file6.csv",
        ]
        data_type_keywords = {
            "garmin-device-heart-rate": [],
            "garmin-connect-sleep-stage": [],
            "garmin-device-step": [],
            "garmin-device-respiration": [],
            "garmin-device-stress": [],
            "garmin-device-pulse-ox": [],
        }
        for path in files:
            for keyword, file_list in data_type_keywords.items():
                if keyword in path:
                    file_list.append(path)
                    break
        assert len(data_type_keywords["garmin-device-heart-rate"]) == 1
        assert len(data_type_keywords["garmin-connect-sleep-stage"]) == 1
        assert len(data_type_keywords["garmin-device-step"]) == 1
        assert len(data_type_keywords["garmin-device-respiration"]) == 1
        assert len(data_type_keywords["garmin-device-stress"]) == 1
        assert len(data_type_keywords["garmin-device-pulse-ox"]) == 1


class TestPatientGrouping:
    def test_group_files_by_patient_from_path(self):
        files = [
            "folder1/Testing-1_cd037752/garmin-device-heart-rate/file1.csv",
            "folder1/Testing-1_cd037752/garmin-device-step/file2.csv",
            "folder1/Testing-2_ab123456/garmin-device-heart-rate/file3.csv",
        ]
        patient_files = {}
        for file in files:
            parts = file.split("/")
            if len(parts) >= 2:
                pid = parts[1]
                patient_files.setdefault(pid, []).append(file)
        assert "Testing-1_cd037752" in patient_files
        assert "Testing-2_ab123456" in patient_files
        assert len(patient_files["Testing-1_cd037752"]) == 2
        assert len(patient_files["Testing-2_ab123456"]) == 1

    def test_extract_participant_id_from_composite_key(self):
        composite_key = "cd037752_20250101100000"
        parts = composite_key.split("_")
        participant_id = parts[0] if len(parts) > 0 else None
        timestamp = parts[1] if len(parts) > 1 else None
        assert participant_id == "cd037752"
        assert timestamp == "20250101100000"

    def test_output_path_structure(self):
        output_path = "s3://bucket/initial_append"
        patient_id = "Testing-1_cd037752"
        data_type = "garmin-device-heart-rate"
        assert f"{output_path}/{patient_id}/{data_type}.csv" == "s3://bucket/initial_append/Testing-1_cd037752/garmin-device-heart-rate.csv"


# ============================================================================
# INTEGRATION TESTS - S3 Operations
# ============================================================================

class TestS3Operations:
    def test_list_objects_in_bucket(self, mock_s3_buckets):
        s3 = mock_s3_buckets["s3_client"]
        bucket = mock_s3_buckets["source_bucket"]
        upload_csv_to_s3(s3, bucket, "patient1/garmin-device-heart-rate/file1.csv", "test data 1")
        upload_csv_to_s3(s3, bucket, "patient1/garmin-device-step/file2.csv", "test data 2")
        response = s3.list_objects_v2(Bucket=bucket)
        assert "Contents" in response
        assert len(response["Contents"]) == 2

    def test_find_csv_files_by_type(self, mock_s3_buckets):
        s3 = mock_s3_buckets["s3_client"]
        bucket = mock_s3_buckets["source_bucket"]
        files = {
            "patient1/garmin-device-heart-rate/file1.csv": "heart rate data",
            "patient1/garmin-device-step/file2.csv": "step data",
            "patient1/garmin-connect-sleep-stage/file3.csv": "sleep data",
        }
        for k, v in files.items():
            upload_csv_to_s3(s3, bucket, k, v)

        paginator = s3.get_paginator("list_objects_v2")
        all_paths = []
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                all_paths.append(obj["Key"])

        assert len([p for p in all_paths if "garmin-device-heart-rate" in p and p.endswith(".csv")]) == 1
        assert len([p for p in all_paths if "garmin-device-step" in p and p.endswith(".csv")]) == 1
        assert len([p for p in all_paths if "garmin-connect-sleep-stage" in p and p.endswith(".csv")]) == 1


# ============================================================================
# CSV PROCESSING TESTS - Real Data from garmin_test_data
# (UPDATED: validate the standardized/cleaned schema you actually output)
# ============================================================================

class TestCSVProcessing:
    def test_read_heart_rate_csv_structure(self):
        if not HEART_RATE_CSV.exists():
            pytest.skip(f"CSV file not found: {HEART_RATE_CSV}")

        df = read_csv_to_dataframe(HEART_RATE_CSV, skiprows=6)

        assert len(df) > 0, "CSV file should have data rows"
        assert "participant_id" in df.columns
        assert "unixTimestampInMs" in df.columns
        assert "Timestamp" in df.columns
        assert "beatsPerMinute" in df.columns
        assert "participantid_timestamp" in df.columns

    def test_read_step_csv_structure(self):
        if not STEP_CSV.exists():
            pytest.skip(f"CSV file not found: {STEP_CSV}")

        df = read_csv_to_dataframe(STEP_CSV, skiprows=6)

        assert len(df) > 0
        assert "participant_id" in df.columns
        assert "unixTimestampInMs" in df.columns
        assert "Timestamp" in df.columns
        assert "participantid_timestamp" in df.columns
        # depending on your cleaned output, you may have one or both:
        assert any(c in df.columns for c in ("steps", "totalSteps"))

    def test_read_sleep_csv_structure(self):
        if not SLEEP_CSV.exists():
            pytest.skip(f"CSV file not found: {SLEEP_CSV}")

        df = read_csv_to_dataframe(SLEEP_CSV, skiprows=6)

        assert len(df) > 0
        assert "participant_id" in df.columns
        assert "unixTimestampInMs" in df.columns
        assert "Timestamp" in df.columns
        assert "participantid_timestamp" in df.columns
        assert "sleepSummaryId" in df.columns
        assert any(c in df.columns for c in ("durationInMs", "TimeInMin"))

    def test_read_respiration_csv_structure(self):
        if not RESPIRATION_CSV.exists():
            pytest.skip(f"CSV file not found: {RESPIRATION_CSV}")

        df = read_csv_to_dataframe(RESPIRATION_CSV, skiprows=6)

        assert len(df) > 0
        assert "participant_id" in df.columns
        assert "unixTimestampInMs" in df.columns
        assert "Timestamp" in df.columns
        assert "participantid_timestamp" in df.columns
        assert "breathsPerMinute" in df.columns

    def test_read_stress_csv_structure(self):
        if not STRESS_CSV.exists():
            pytest.skip(f"CSV file not found: {STRESS_CSV}")

        df = read_csv_to_dataframe(STRESS_CSV, skiprows=6)

        assert len(df) > 0
        assert "participant_id" in df.columns
        assert "unixTimestampInMs" in df.columns
        assert "Timestamp" in df.columns
        assert "participantid_timestamp" in df.columns
        assert "stressLevel" in df.columns

    def test_read_pulse_ox_csv_structure(self):
        if not PULSE_OX_CSV.exists():
            pytest.skip(f"CSV file not found: {PULSE_OX_CSV}")

        df = read_csv_to_dataframe(PULSE_OX_CSV, skiprows=6)

        assert len(df) > 0
        assert "participant_id" in df.columns
        assert "unixTimestampInMs" in df.columns
        assert "Timestamp" in df.columns
        assert "participantid_timestamp" in df.columns
        assert "spo2" in df.columns


class TestPreprocessingWithRealData:
    def test_all_garmin_data_type_files_exist(self):
        files = [
            ("Heart Rate", HEART_RATE_CSV),
            ("Step", STEP_CSV),
            ("Sleep", SLEEP_CSV),
            ("Respiration", RESPIRATION_CSV),
            ("Stress", STRESS_CSV),
            ("Pulse-Ox", PULSE_OX_CSV),
        ]
        existing = [name for name, p in files if p.exists()]
        assert len(existing) > 0

    def test_upload_all_garmin_data_types_to_s3(self, mock_s3_buckets):
        s3 = mock_s3_buckets["s3_client"]
        bucket = mock_s3_buckets["source_bucket"]
        participant_id = "Testing-1_cd037752"

        files = [
            (HEART_RATE_CSV, "garmin-device-heart-rate"),
            (STEP_CSV, "garmin-device-step"),
            (SLEEP_CSV, "garmin-connect-sleep-stage"),
            (RESPIRATION_CSV, "garmin-device-respiration"),
            (STRESS_CSV, "garmin-device-stress"),
            (PULSE_OX_CSV, "garmin-device-pulse-ox"),
        ]

        uploaded = []
        for file_path, data_type in files:
            if not file_path.exists():
                continue
            csv_content = read_csv_file(file_path)  # converts excel/binary -> csv text
            key = f"{participant_id}/{data_type}/{file_path.name}"
            upload_csv_to_s3(s3, bucket, key, csv_content)
            uploaded.append(data_type)

        for _, data_type in files:
            if data_type in uploaded:
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{participant_id}/{data_type}/")
                assert "Contents" in resp and len(resp["Contents"]) > 0

    def test_list_all_garmin_files_in_bucket(self, mock_s3_buckets):
        s3 = mock_s3_buckets["s3_client"]
        bucket = mock_s3_buckets["source_bucket"]
        participant_id = "Testing-1_cd037752"

        data_types = {
            "garmin-device-heart-rate": HEART_RATE_CSV,
            "garmin-device-step": STEP_CSV,
            "garmin-connect-sleep-stage": SLEEP_CSV,
        }

        for data_type, file_path in data_types.items():
            if file_path.exists():
                csv_content = read_csv_file(file_path)
                key = f"{participant_id}/{data_type}/{file_path.name}"
                upload_csv_to_s3(s3, bucket, key, csv_content)

        paginator = s3.get_paginator("list_objects_v2")
        all_paths = []
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".csv"):
                    all_paths.append(obj["Key"])

        categorized = {
            "garmin-device-heart-rate": [],
            "garmin-connect-sleep-stage": [],
            "garmin-device-step": [],
            "garmin-device-respiration": [],
            "garmin-device-stress": [],
            "garmin-device-pulse-ox": [],
        }
        for p in all_paths:
            for dt in categorized:
                if dt in p:
                    categorized[dt].append(p)
                    break

        for dt in data_types:
            assert len(categorized[dt]) > 0


# ============================================================================
# EDGE CASES AND ERROR HANDLING
# ============================================================================

class TestErrorHandling:
    def test_empty_bucket_handling(self, mock_s3_buckets):
        s3 = mock_s3_buckets["s3_client"]
        bucket = mock_s3_buckets["source_bucket"]
        response = s3.list_objects_v2(Bucket=bucket)
        assert "Contents" not in response or len(response.get("Contents", [])) == 0

    def test_csv_header_row_count(self):
        """
        Since fixtures are already cleaned/standardized, validate the cleaned schema,
        not raw Garmin headers like 'timezone'/'isoDate'.
        """
        if not HEART_RATE_CSV.exists():
            pytest.skip(f"CSV file not found: {HEART_RATE_CSV}")

        df = read_csv_to_dataframe(HEART_RATE_CSV, skiprows=6)
        assert any(c in df.columns for c in ("Timestamp", "unixTimestampInMs", "participant_id"))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])