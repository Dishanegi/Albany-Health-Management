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
SOURCE_BUCKET = os.environ.get("SOURCE_BUCKET")
DESTINATION_BUCKET = os.environ.get("DESTINATION_BUCKET")
delete_detail_type = os.environ.get("EVENTBRIDGE_DELETE_DETAIL_TYPE")
event_bus_name = os.environ.get("EVENTBRIDGE_BUS_NAME", "default")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _decode_bytes(b: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            pass
    raise ValueError("Unable to decode input file with any known encoding")


def _get_earliest_date(df: pd.DataFrame) -> str:
    """Get the earliest isoDate from the DataFrame as a compact string e.g. '20260101'"""
    if "isoDate" not in df.columns:
        return "unknown"
    try:
        earliest = pd.to_datetime(df["isoDate"], errors="coerce").dropna().min()
        if pd.isnull(earliest):
            return "unknown"
        return earliest.strftime("%Y%m%d")
    except Exception:
        return "unknown"


def _get_patient_folders(bucket: str) -> list:
    """
    List all top-level patient folders in the source bucket.
    e.g. ['Testing-1_cd037752/', 'Testing-2_ab123456/']
    """
    paginator = s3.get_paginator("list_objects_v2")
    result = paginator.paginate(Bucket=bucket, Delimiter="/")

    patient_folders = []
    for page in result:
        for prefix in page.get("CommonPrefixes", []):
            patient_folders.append(prefix["Prefix"].rstrip("/"))

    logger.info("Found %d patient folders", len(patient_folders))
    return patient_folders


def _get_csv_files(bucket: str, patient_folder: str) -> list:
    """
    List all cleaned CSV files inside a patient folder.
    e.g. ['Testing-1_cd037752/260122_file_cleaned.csv', ...]
    """
    paginator = s3.get_paginator("list_objects_v2")
    result = paginator.paginate(Bucket=bucket, Prefix=f"{patient_folder}/")

    csv_files = []
    for page in result:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("_cleaned.csv"):
                csv_files.append(key)

    logger.info("Found %d CSV files for patient %s", len(csv_files), patient_folder)
    return csv_files


def _merge_files(bucket: str, csv_files: list) -> pd.DataFrame:
    """
    Read all CSV files for a patient and merge into a single DataFrame.
    Zero-byte placeholder files (written when source was empty/unparseable) are skipped.
    """
    dfs = []
    for key in csv_files:
        logger.info("Reading file: %s", key)
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw_bytes = obj["Body"].read()
        if not raw_bytes.strip():
            logger.info("Skipping empty placeholder file: %s", key)
            continue
        try:
            raw_text = _decode_bytes(raw_bytes)
            df = pd.read_csv(io.StringIO(raw_text), dtype=str, keep_default_na=False)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            logger.warning("Could not read %s — skipping. Reason: %s", key, e)

    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)

    # Sort by isoDate if present for chronological order
    if "isoDate" in merged.columns:
        merged = merged.sort_values("isoDate", kind="stable").reset_index(drop=True)

    logger.info("Merged %d files into %d rows", len(dfs), len(merged))
    return merged


def _upload_merged(bucket: str, patient_folder: str, merged_df: pd.DataFrame) -> str:
    """
    Upload merged CSV to destination bucket.
    If a merged file already exists for this patient, append new data to it.
    File is named with the earliest date in the data e.g. Testing-1_cd037752/20260101_merged.csv
    """
    earliest_date = _get_earliest_date(merged_df)
    out_key = f"{patient_folder}/{earliest_date}_merged.csv"

    # Check if a merged file already exists for this patient
    paginator = s3.get_paginator("list_objects_v2")
    existing_files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{patient_folder}/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("_merged.csv"):
                existing_files.append(obj["Key"])

    if existing_files:
        # Append to the existing merged file
        existing_key = existing_files[0]
        logger.info("Existing merged file found: %s — appending", existing_key)

        obj = s3.get_object(Bucket=bucket, Key=existing_key)
        raw_text = _decode_bytes(obj["Body"].read())
        existing_df = pd.read_csv(io.StringIO(raw_text), dtype=str, keep_default_na=False)

        combined_df = pd.concat([existing_df, merged_df], ignore_index=True)

        # Deduplicate and re-sort
        if "isoDate" in combined_df.columns:
            combined_df = combined_df.drop_duplicates()
            combined_df = combined_df.sort_values("isoDate", kind="stable").reset_index(drop=True)

        # Use the earliest date from the combined data for the filename
        earliest_date = _get_earliest_date(combined_df)
        out_key = f"{patient_folder}/{earliest_date}_merged.csv"

        # Delete old file if the name has changed
        if existing_key != out_key:
            s3.delete_object(Bucket=bucket, Key=existing_key)
            logger.info("Deleted old merged file: %s", existing_key)

        merged_df = combined_df

    buf = io.StringIO()
    merged_df.to_csv(buf, index=False)

    s3.put_object(
        Bucket=bucket,
        Key=out_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    logger.info("Uploaded merged file -> s3://%s/%s", bucket, out_key)
    return out_key


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Triggered by EventBridge.
    Reads all cleaned CSVs per patient from source bucket,
    merges them into one file, and dumps to destination bucket.
    If patient already exists in destination, appends new data.
    File is named with the earliest date in the merged data.
    """
    logger.info("Starting survey data merge job")

    source_bucket = SOURCE_BUCKET
    dest_bucket = DESTINATION_BUCKET

    if not source_bucket or not dest_bucket:
        raise ValueError("SOURCE_BUCKET and DESTINATION_BUCKET environment variables must be set")

    # Step 1: Get all patient folders
    patient_folders = _get_patient_folders(source_bucket)

    if not patient_folders:
        logger.warning("No patient folders found in bucket %s", source_bucket)
        return {"statusCode": 200, "message": "No patient folders found"}

    results = []
    failed = []

    # Step 2: Process each patient
    for patient_folder in patient_folders:
        try:
            # Get all cleaned CSV files for this patient
            csv_files = _get_csv_files(source_bucket, patient_folder)

            if not csv_files:
                logger.warning("No cleaned CSV files found for patient %s — skipping", patient_folder)
                continue

            # Merge all files into one
            merged_df = _merge_files(source_bucket, csv_files)

            if merged_df.empty:
                logger.warning("Merged DataFrame is empty for patient %s — skipping", patient_folder)
                continue

            # Upload merged file to destination bucket (appends if already exists)
            out_key = _upload_merged(dest_bucket, patient_folder, merged_df)

            results.append({
                "patient_folder": patient_folder,
                "files_merged": len(csv_files),
                "rows_written": len(merged_df),
                "output_key": out_key,
            })

            logger.info(
                "Patient %s: merged %d files, %d rows -> %s",
                patient_folder, len(csv_files), len(merged_df), out_key,
            )

        except Exception as e:
            logger.error("Failed to process patient %s: %s", patient_folder, str(e))
            failed.append({"patient_folder": patient_folder, "error": str(e)})

    # Step 3: Trigger EventBridge rule to delete processing files
    if results:
        try:
            eventbridge = boto3.client("events")

            if not delete_detail_type:
                raise ValueError("EVENTBRIDGE_DELETE_DETAIL_TYPE environment variable must be set")

            entry = {
                "Source": "lambda",
                "DetailType": delete_detail_type,
                "Detail": json.dumps({
                    "processedPatients": [r["patient_folder"] for r in results],
                    "processedCount": len(results),
                    "sourceBucket": source_bucket,
                }),
            }

            if event_bus_name and event_bus_name != "default":
                entry["EventBusName"] = event_bus_name

            response = eventbridge.put_events(Entries=[entry])

            failed_events = response.get("FailedEntryCount", 0)
            if failed_events > 0:
                logger.error("Failed to send delete trigger event to EventBridge: %s", response)
            else:
                logger.info(
                    "Successfully triggered delete rule via EventBridge with detail-type '%s'",
                    delete_detail_type,
                )

        except Exception as e:
            logger.error("Failed to trigger EventBridge delete rule: %s", str(e))

    logger.info(
        "Merge job complete. Processed: %d patients, Failed: %d patients",
        len(results), len(failed),
    )

    return {
        "statusCode": 200,
        "processed": len(results),
        "failed": len(failed),
        "results": results,
        "failures": failed if failed else None,
    }