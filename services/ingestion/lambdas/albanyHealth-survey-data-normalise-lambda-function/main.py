import os
import io
import json
import logging
from typing import Any, Dict
from datetime import datetime

import boto3
import pandas as pd

# ---------- AWS clients ----------
s3 = boto3.client("s3")

# ---------- Config ----------
OUTPUT_BUCKET = os.environ.get("DESTINATION_BUCKET")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _decode_bytes(b: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            pass
    raise ValueError("Unable to decode input file with any known encoding")


def _get_time_of_day(iso_date_str: str) -> str:
    """
    Determine time of day based on ISO date timestamp.
    Returns: 'morning', 'noon', 'evening', or 'night'
    """
    try:
        dt = datetime.fromisoformat(iso_date_str.replace('Z', '+00:00'))
        hour = dt.hour
        
        if 5 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 17:
            return 'noon'
        elif 17 <= hour < 21:
            return 'evening'
        else:
            return 'night'
    except Exception as e:
        logger.warning(f"Could not parse date '{iso_date_str}': {e}")
        return 'unknown'


def _format_iso_date(iso_date_str: str) -> str:
    """
    Format ISO date to a more readable format.
    """
    try:
        dt = datetime.fromisoformat(iso_date_str.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.warning(f"Could not format date '{iso_date_str}': {e}")
        return iso_date_str


def _clean(raw_text: str) -> pd.DataFrame:
    """
    1. Extract participantId from preamble metadata (lines 4-5).
    2. Skip the 6-line preamble block (lines 1-6).
    3. Skip the key/question table.
    4. Parse from the 'timezone' header onward.
    """
    lines = raw_text.splitlines()

    # ============================================================================
    # STEP 1: Extract participantId from preamble BEFORE anything else
    # ============================================================================
    patient_id = ''
    for i, line in enumerate(lines):
        stripped = line.strip()
        # The preamble header row contains 'participantId' as a column name
        if stripped.startswith('projectId') and 'participantId' in stripped:
            # The very next non-empty line is the values row
            meta_headers = [h.strip() for h in stripped.split(',')]
            pid_index = meta_headers.index('participantId')
            
            # Find the next non-empty line (the values row)
            for j in range(i + 1, len(lines)):
                value_line = lines[j].strip()
                if value_line:
                    # Parse carefully — values may be quoted CSV
                    import csv
                    reader = csv.reader([value_line])
                    meta_values = next(reader)
                    if pid_index < len(meta_values):
                        patient_id = meta_values[pid_index].strip()
                    break
            break

    logger.info(f"Extracted participantId from preamble: '{patient_id}'")

    # ============================================================================
    # STEP 2: Find where the actual data table starts ('timezone' header)
    # ============================================================================
    data_start = None
    for i, line in enumerate(lines):
        if line.strip().startswith('timezone'):
            data_start = i
            logger.info(f"Found data table header at line {i}: {line.strip()[:100]}")
            break

    if data_start is None:
        raise ValueError("Could not find data table — expected a line starting with 'timezone'")

    data_block = "\n".join(lines[data_start:])

    # ============================================================================
    # STEP 3: Parse the data table
    # ============================================================================
    df = pd.read_csv(
        io.StringIO(data_block),
        dtype=str,
        keep_default_na=False
    )

    logger.info(f"DataFrame loaded: {df.shape[0]} rows × {df.shape[1]} columns")
    logger.info(f"Columns: {df.columns.tolist()}")

    # ============================================================================
    # STEP 4: Attach the extracted patient_id as a column
    # ============================================================================
    df['patient_id'] = patient_id
    logger.info(f"Attached patient_id='{patient_id}' to all {len(df)} rows")

    # ============================================================================
    # STEP 5: Drop unixTimestampInMs
    # ============================================================================
    if 'unixTimestampInMs' in df.columns:
        df = df.drop(columns=['unixTimestampInMs'])
        logger.info("Dropped 'unixTimestampInMs'")

    # ============================================================================
    # STEP 6: Transform isoDate and add time_of_day
    # ============================================================================
    if 'isoDate' in df.columns:
        df['time_of_day'] = df['isoDate'].apply(_get_time_of_day)
        df['isoDate'] = df['isoDate'].apply(_format_iso_date)
        logger.info("Transformed 'isoDate' and added 'time_of_day'")
    else:
        logger.warning("'isoDate' column not found")

    # ============================================================================
    # STEP 7: Reorder — patient_id first
    # ============================================================================
    cols = df.columns.tolist()
    cols.remove('patient_id')
    df = df[['patient_id'] + cols]
    logger.info("Reordered columns with patient_id first")

    # ============================================================================
    # FINAL VALIDATION
    # ============================================================================
    logger.info(f"✓ Final shape: {df.shape}")
    logger.info(f"✓ Final columns: {df.columns.tolist()}")
    logger.info(f"✓✓✓ patient_id value: '{patient_id}' on all {len(df)} rows")

    return df


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    record = event["Records"][0]

    # Unwrap SQS message to get S3 details
    message = json.loads(record["body"])
    bucket = message["source_bucket"]
    key = message["object_key"]

    logger.info("Processing s3://%s/%s", bucket, key)

    # Download
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw_text = _decode_bytes(obj["Body"].read())

    # Clean
    clean_df = _clean(raw_text)

    # Upload
    out_bucket = OUTPUT_BUCKET or bucket
    
    # Extract the folder name
    path_parts = key.split('/')
    if len(path_parts) >= 3:
        folder_name = path_parts[2]
    else:
        folder_name = 'unknown'
    
    # Create output key
    filename = key.split('/')[-1]
    out_key = f"{folder_name}/{filename.rsplit('.', 1)[0]}_cleaned.csv"    

    buf = io.StringIO()
    clean_df.to_csv(buf, index=False)

    s3.put_object(
        Bucket=out_bucket,
        Key=out_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    logger.info("Wrote cleaned file -> s3://%s/%s (%d rows)", out_bucket, out_key, len(clean_df))

    return {
        "statusCode": 200,
        "input": {"bucket": bucket, "key": key},
        "output": {"bucket": out_bucket, "key": out_key},
        "rows_written": int(len(clean_df)),
    }