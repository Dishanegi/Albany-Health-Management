import sys
import boto3
import traceback
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, split

# ── Logging helpers ──────────────────────────────────────────────────────────

def log_error(message, error_details=None):
    """Log errors to S3 for debugging"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_message = f"[{timestamp}] ERROR: {message}\n"
    if error_details:
        log_message += f"Details: {error_details}\n"
        log_message += f"Traceback: {traceback.format_exc()}\n"
    log_key = f"error_logs/error_log_{timestamp}.txt"
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=source_bucket_name, Key=log_key, Body=log_message)
        print(f"Error logged to s3://{source_bucket_name}/{log_key}")
    except Exception as e:
        print(f"Failed to write error log: {str(e)}")


def log_info(message):
    """Log information to console"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"[{timestamp}] INFO: {message}")


# ── S3 helpers ───────────────────────────────────────────────────────────────

def list_csv_files_in_folder(s3_client, bucket, prefix):
    """
    Return every .csv key under bucket/prefix using pagination.
    Works correctly whether the folder contains 1 file or many.
    """
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])
    return keys


def delete_folder(s3_client, bucket, folder_prefix):
    """Delete all objects under folder_prefix."""
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=folder_prefix):
            for obj in page.get("Contents", []):
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
        log_info(f"Deleted folder: {folder_prefix}")
    except Exception as e:
        log_error(f"Error deleting folder {folder_prefix}", str(e))


# ── Data type registry ────────────────────────────────────────────────────────
# Every entry here is treated identically: all CSV files found in
# {patient_id}/{DATA_TYPE}/ are stacked (unioned) into one output file.
# Sleep intentionally appears here — it supports multiple files per patient
# exactly like heart-rate, step, etc.

ALL_DATA_TYPES = [
    "garmin-device-heart-rate",
    "garmin-connect-sleep-stage",   # ← supports 1..N files per patient
    "garmin-device-step",
    "garmin-device-respiration",
    "garmin-device-stress",
    "garmin-device-pulse-ox",
]


# ── Spark processing ──────────────────────────────────────────────────────────

def process_files_by_type(glue_context, s3_bucket, file_keys, data_type_label):
    """
    Read every CSV file in file_keys, union them all into one DataFrame,
    and return it.  Works for any number of files (1 or many).
    """
    if not file_keys:
        return None

    log_info(f"Stacking {len(file_keys)} file(s) for '{data_type_label}'")

    combined_df = None
    for idx, key in enumerate(file_keys):
        file_path = f"s3://{s3_bucket}/{key}"
        log_info(f"  Reading [{idx + 1}/{len(file_keys)}]: {file_path}")

        # Use a unique ctx key so Glue's bookmark system tracks each file separately
        ctx_key = f"{data_type_label.replace(' ', '_').replace('/', '_')}_{idx}"

        current_df = glue_context.create_dynamic_frame.from_options(
            format_options={
                "quoteChar": '"',
                "withHeader": True,
                "separator": ",",
            },
            connection_type="s3",
            format="csv",
            connection_options={"paths": [file_path]},
            transformation_ctx=ctx_key,
        ).toDF()

        combined_df = current_df if combined_df is None else combined_df.union(current_df)

    if combined_df is None:
        log_error(f"No data loaded for '{data_type_label}'")
        return None

    log_info(f"Schema for '{data_type_label}': {combined_df.schema.simpleString()}")

    # Extract participant_id and timestamp from composite key when present
    if "participantid_timestamp" in combined_df.columns:
        combined_df = combined_df.withColumn(
            "participant_id",
            split(col("participantid_timestamp"), "_").getItem(0),
        ).withColumn(
            "timestamp",
            split(col("participantid_timestamp"), "_").getItem(1),
        )

    log_info(f"Total rows for '{data_type_label}': {combined_df.count()}")
    return combined_df


def write_single_csv(s3_client, df, s3_bucket, output_key, temp_prefix):
    """
    Write df as a single CSV file at s3://s3_bucket/output_key.
    Uses a temporary location + coalesce(1) then renames the part file.
    """
    spark_path = f"s3://{s3_bucket}/{temp_prefix}"

    (df.coalesce(1)
       .write
       .mode("overwrite")
       .options(header="true", delimiter=",", quote='"')
       .csv(spark_path))

    # Find the single part file Spark wrote
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=temp_prefix)
    part_file = next(
        (obj["Key"] for obj in response.get("Contents", [])
         if obj["Key"].endswith(".csv") or "part-" in obj["Key"]),
        None,
    )

    if not part_file:
        raise RuntimeError(f"No part file found under s3://{s3_bucket}/{temp_prefix}")

    # Copy to the final destination
    s3_client.copy_object(
        Bucket=s3_bucket,
        CopySource={"Bucket": s3_bucket, "Key": part_file},
        Key=output_key,
    )

    # Remove the temporary directory
    temp_objects = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=temp_prefix)
    for obj in temp_objects.get("Contents", []):
        s3_client.delete_object(Bucket=s3_bucket, Key=obj["Key"])

    log_info(f"Written single CSV: s3://{s3_bucket}/{output_key}")


# ── Main job ──────────────────────────────────────────────────────────────────

try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    def get_optional_arg(key, default):
        try:
            idx = sys.argv.index(f"--{key}")
            if idx + 1 < len(sys.argv):
                return sys.argv[idx + 1]
        except ValueError:
            pass
        return default

    source_bucket_name = get_optional_arg("SOURCE_BUCKET", "albanyhealthprocessed-s3bucket-dev")
    target_bucket_name = get_optional_arg("TARGET_BUCKET", "albanyhealthmerged-s3bucket-dev")
    output_path = f"s3://{source_bucket_name}/initial_append"

    log_info("Job initialised successfully")
    log_info(f"Source bucket : {source_bucket_name}")
    log_info(f"Target bucket : {target_bucket_name}")
    log_info(f"Output path   : {output_path}")

    s3_client = boto3.client("s3")

    # ── Step 1: Discover all patient folders (top-level prefixes) ────────────
    # A "patient folder" is any top-level common prefix, e.g. "Testing-1_cd037752/"
    log_info("Discovering patient folders in bucket …")
    top_level_response = s3_client.list_objects_v2(
        Bucket=source_bucket_name,
        Delimiter="/"
    )
    patient_folders = [
        cp["Prefix"].rstrip("/")
        for cp in top_level_response.get("CommonPrefixes", [])
        if not cp["Prefix"].startswith("initial_append")   # skip output folder
        and not cp["Prefix"].startswith("error_logs")      # skip log folder
    ]

    if not patient_folders:
        log_info("No patient folders found — nothing to process.")
        job.commit()
        sys.exit(0)

    log_info(f"Found {len(patient_folders)} patient folder(s): {patient_folders}")

    # ── Step 2: For each patient × data_type, collect ALL csv files ──────────
    # This is the key change: we scan the exact subfolder for each data type,
    # so all files (whether 1 or 7+) are always included — including sleep.
    patient_data = {}   # { patient_id: { data_type: [s3_key, …] } }

    for patient_id in patient_folders:
        patient_data[patient_id] = {}
        for data_type in ALL_DATA_TYPES:
            prefix = f"{patient_id}/{data_type}/"
            keys = list_csv_files_in_folder(s3_client, source_bucket_name, prefix)
            if keys:
                patient_data[patient_id][data_type] = keys
                log_info(
                    f"  {patient_id} / {data_type}: {len(keys)} file(s) found"
                )
            else:
                log_info(
                    f"  {patient_id} / {data_type}: no files found (skipping)"
                )

    # ── Step 3: Process and write each patient × data_type combination ───────
    processed_patient_folders = set()

    for patient_id, type_map in patient_data.items():
        if not type_map:
            log_info(f"No recognised data files for {patient_id} — skipping")
            continue

        for data_type, file_keys in type_map.items():
            label = f"{data_type} / {patient_id}"
            log_info(f"Processing: {label}  ({len(file_keys)} file(s))")

            df = process_files_by_type(glueContext, source_bucket_name, file_keys, label)

            if df is not None:
                output_key  = f"initial_append/{patient_id}/{data_type}.csv"
                temp_prefix = f"initial_append/{patient_id}/{data_type}_temp/"

                try:
                    write_single_csv(s3_client, df, source_bucket_name, output_key, temp_prefix)
                    log_info(f"Saved: s3://{source_bucket_name}/{output_key}")
                    processed_patient_folders.add(patient_id)
                except Exception as e:
                    log_error(f"Error writing CSV for {label}", str(e))
                    raise

    # ── Step 4: Delete original patient folders now all types are processed ──
    log_info("Removing original patient folders from processed bucket …")
    for patient_id in processed_patient_folders:
        delete_folder(s3_client, source_bucket_name, f"{patient_id}/")
        log_info(f"Deleted original folder: {patient_id}/")

    job.commit()
    log_info("Job completed successfully")

except Exception as e:
    log_error("Fatal error in main job flow", str(e))
    raise e
