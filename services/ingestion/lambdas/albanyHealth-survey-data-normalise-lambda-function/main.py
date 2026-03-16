import os
import io
import json
import logging
from typing import Any, Dict

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


def _get_question_map(lines: list) -> dict:
    """
    Parse the key table to build a map of column name -> question description.
    e.g. {'1_1': 'Are you feeling pain right now?', '1_2': '...'}
    """
    key_header = next(
        (i for i, l in enumerate(lines) if l.strip().startswith("sectionIndex")),
        None,
    )
    data_header = next(
        (i for i, l in enumerate(lines) if l.strip().startswith("timezone")),
        None,
    )
    if key_header is None or data_header is None:
        return {}

    key_block = "\n".join(l for l in lines[key_header:data_header] if l.strip())
    key_df = pd.read_csv(
        io.StringIO(key_block),
        dtype=str,
        keep_default_na=False,
        on_bad_lines="skip",
    )

    question_map = {}
    for _, row in key_df.iterrows():
        col_name = f"{row['sectionIndex']}_{row['questionIndex']}"  # e.g. '1_1'
        question_map[col_name] = row.get("questionDescription", row.get("questionName", ""))

    return question_map


def _clean(raw_text: str) -> pd.DataFrame:
    """
    Strip all metadata/key-table preamble, return the data table
    with answer columns renamed to their question descriptions.
    """
    lines = raw_text.splitlines()

    # Build question description map from key table
    question_map = _get_question_map(lines)

    # Find the line where the actual data table starts
    data_start = next(
        (i for i, line in enumerate(lines) if line.strip().startswith("timezone")),
        None,
    )
    if data_start is None:
        raise ValueError(
            "Could not find data table — expected a line starting with 'timezone'"
        )

    data_block = "\n".join(lines[data_start:])
    df = pd.read_csv(
        io.StringIO(data_block),
        dtype=str,
        keep_default_na=False,
        on_bad_lines="skip",
    )

    # Drop completely empty rows if any
    df = df.dropna(how="all").reset_index(drop=True)

    # ✅ Rename answer columns using question descriptions
    df = df.rename(columns=question_map)

    logger.info("Cleaned data: %d rows x %d columns", len(df), len(df.columns))
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
    patient_folder = key.split('/')[0]  # 'Testing-1_cd037752'
    out_key = f"{patient_folder}/{key.split('/')[-1].rsplit('.', 1)[0]}_cleaned.csv"    

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