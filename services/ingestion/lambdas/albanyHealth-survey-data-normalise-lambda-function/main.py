import os
import io
import re
import csv
import logging
from typing import Tuple

import boto3
import pandas as pd

# ---------- AWS clients ----------
s3 = boto3.client("s3")

# ---------- Config ----------
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET")          # optional; default = same as input bucket
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "normalized/")

# Match Labfront answer columns like "1_1", "1_2", ...
ANSWER_COL_RE = re.compile(r"^\d+_\d+$")

OUT_COLS = [
    "participant_id",
    "submission_ts",
    "timezone",
    "section_index",
    "question_index",
    "question_id",
    "question_text",
    "answer_code",
    "answer_text",
]

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------- Helpers ----------
def _decode_bytes(b: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            pass
    raise ValueError("Unable to decode input file")


def _read_meta(lines) -> dict:
    """
    Find meta table starting with 'projectId,...' and parse the next row safely.
    Uses Python csv.reader (robust to odd quoting when parsing a single line).
    """
    for i, ln in enumerate(lines):
        if ln.strip().startswith("projectId"):
            if i + 1 >= len(lines):
                return {}

            header = next(csv.reader([lines[i]]))
            row = next(csv.reader([lines[i + 1]]))

            # pad/truncate row to match header length
            if len(row) < len(header):
                row += [""] * (len(header) - len(row))
            if len(row) > len(header):
                row = row[:len(header)]

            return dict(zip(header, row))

    return {}


def _read_key_table(lines) -> Tuple[pd.DataFrame, int]:
    """
    Reads the key table between:
      sectionIndex,... (header)
    and
      timezone,... (data header)
    Returns (key_df, data_start_line_index).
    """
    key_start = None
    data_start = None

    for i, ln in enumerate(lines):
        s = ln.strip()
        if key_start is None and s.startswith("sectionIndex"):
            key_start = i
        if s.startswith("timezone"):
            data_start = i
            break

    if key_start is None:
        raise ValueError("Missing key table header (sectionIndex...)")
    if data_start is None:
        raise ValueError("Missing data table header (timezone...)")

    key_csv = "\n".join([l for l in lines[key_start:data_start] if l.strip() != ""])

    # Python engine is more tolerant of odd quotes/commas in long text fields
    key_df = pd.read_csv(
        io.StringIO(key_csv),
        dtype=str,
        engine="python",
        keep_default_na=False,
        on_bad_lines="skip",
    )

    return key_df, data_start


def _read_data_table(lines, data_start: int) -> pd.DataFrame:
    """
    Reads the responses table starting at 'timezone,...' until EOF.
    """
    data_csv = "\n".join([l for l in lines[data_start:] if l.strip() != ""])

    data_df = pd.read_csv(
        io.StringIO(data_csv),
        dtype=str,
        engine="python",
        keep_default_na=False,
        on_bad_lines="skip",
    )

    return data_df


def _normalize_with_pandas(raw_text: str) -> pd.DataFrame:
    """
    Convert Labfront export from wide format to normalized long format (AFTER).
    """
    lines = raw_text.splitlines()

    # ---- Meta ----
    meta = _read_meta(lines)
    participant_id = str(meta.get("participantId") or meta.get("participant_id") or "").strip()

    # ---- Key table + Data table ----
    key_df, data_start = _read_key_table(lines)
    data_df = _read_data_table(lines, data_start)

    # ---- Decide which column contains question text ----
    # Labfront exports vary: questionDescription vs questionDescript vs questionName
    if "questionDescription" in key_df.columns:
        qtext_col = "questionDescription"
    elif "questionDescript" in key_df.columns:
        qtext_col = "questionDescript"
    elif "questionName" in key_df.columns:
        qtext_col = "questionName"
    else:
        qtext_col = None

    # ---- Build question dimension (section_index, question_index -> id, text) ----
    qdim = key_df.copy()

    if "sectionIndex" not in qdim.columns or "questionIndex" not in qdim.columns:
        raise ValueError("Key table missing sectionIndex/questionIndex columns")

    qdim["section_index"] = pd.to_numeric(qdim["sectionIndex"], errors="coerce")
    qdim["question_index"] = pd.to_numeric(qdim["questionIndex"], errors="coerce")

    qdim["question_id"] = qdim["questionId"] if "questionId" in qdim.columns else ""
    qdim["question_text"] = qdim[qtext_col] if qtext_col else ""

    qbase = (
        qdim[["section_index", "question_index", "question_id", "question_text"]]
        .dropna(subset=["section_index", "question_index"])
        .drop_duplicates()
    )

    # ---- Build options long table: (section_index, question_index, answer_code -> answer_text) ----
    option_cols = [c for c in qdim.columns if re.match(r"^option[1-5]Name$", str(c))]
    if option_cols:
        opt_long = (
            qdim[["section_index", "question_index"] + option_cols]
            .melt(id_vars=["section_index", "question_index"], var_name="opt_col", value_name="answer_text")
        )
        opt_long["answer_code"] = (
            opt_long["opt_col"].str.extract(r"option(\d)Name")[0]
            .astype("Int64")
        )
        opt_long["answer_text"] = opt_long["answer_text"].astype(str).str.strip()
        opt_long = opt_long[opt_long["answer_text"] != ""][["section_index", "question_index", "answer_code", "answer_text"]]
    else:
        opt_long = pd.DataFrame(columns=["section_index", "question_index", "answer_code", "answer_text"])

    # ---- Melt response table from wide to long ----
    answer_cols = [c for c in data_df.columns if ANSWER_COL_RE.match(str(c))]
    if not answer_cols:
        raise ValueError("No answer columns found like '1_1', '1_2', ... in data table")

    id_vars = []
    for c in ["timezone", "isoDate"]:
        if c in data_df.columns:
            id_vars.append(c)

    long_df = data_df.melt(
        id_vars=id_vars,
        value_vars=answer_cols,
        var_name="section_question",
        value_name="answer_code_raw",
    )

    # Split "1_2" => section_index=1, question_index=2
    sq = long_df["section_question"].astype(str).str.split("_", expand=True)
    long_df["section_index"] = pd.to_numeric(sq[0], errors="coerce")
    long_df["question_index"] = pd.to_numeric(sq[1], errors="coerce")

    # Clean answer_code: "", "2", "2.0" => Int64
    long_df["answer_code_raw"] = long_df["answer_code_raw"].astype(str).str.strip()
    long_df.loc[long_df["answer_code_raw"] == "", "answer_code_raw"] = pd.NA
    long_df["answer_code"] = pd.to_numeric(long_df["answer_code_raw"], errors="coerce").round().astype("Int64")

    # Attach participant_id + submission_ts
    long_df["participant_id"] = participant_id
    long_df["submission_ts"] = long_df["isoDate"] if "isoDate" in long_df.columns else ""
    if "timezone" not in long_df.columns:
        long_df["timezone"] = ""

    # Join questions
    long_df = long_df.merge(qbase, on=["section_index", "question_index"], how="left")

    # Join option text
    long_df = long_df.merge(
        opt_long,
        on=["section_index", "question_index", "answer_code"],
        how="left",
    )

    # Final cleanup
    for c in ["question_id", "question_text", "answer_text"]:
        if c in long_df.columns:
            long_df[c] = long_df[c].fillna("")

    out = long_df[OUT_COLS].sort_values(
        ["participant_id", "submission_ts", "section_index", "question_index"],
        kind="stable",
    )

    return out


# ---------- Lambda entry ----------
def lambda_handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    logger.info("Processing S3 object s3://%s/%s", bucket, key)

    obj = s3.get_object(Bucket=bucket, Key=key)
    raw_text = _decode_bytes(obj["Body"].read())

    # (Optional) log a few lines to debug formats safely
    # logger.info("First 15 lines:\n%s", "\n".join(raw_text.splitlines()[:15]))

    out_df = _normalize_with_pandas(raw_text)

    out_bucket = OUTPUT_BUCKET or bucket
    base = key.split("/")[-1]
    out_key = f"{OUTPUT_PREFIX}{base.rsplit('.', 1)[0]}_normalized.csv"

    buf = io.StringIO()
    out_df.to_csv(buf, index=False)

    s3.put_object(
        Bucket=out_bucket,
        Key=out_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    logger.info("Wrote normalized file to s3://%s/%s rows=%d", out_bucket, out_key, len(out_df))

    return {
        "statusCode": 200,
        "input": {"bucket": bucket, "key": key},
        "output": {"bucket": out_bucket, "key": out_key},
        "rows_written": int(len(out_df)),
    }



