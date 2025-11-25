import sys
import boto3
import traceback
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit, col, split, when
import pyspark.sql.functions as F

# Initialize error logging
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

# Simple info logging to console instead of S3
def log_info(message):
    """Log information to console"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"[{timestamp}] INFO: {message}")

try:
    # Initialize the job
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # Parse optional arguments from sys.argv (default arguments from CDK)
    def get_optional_arg(key, default):
        """Get optional argument from sys.argv"""
        try:
            idx = sys.argv.index(f"--{key}")
            if idx + 1 < len(sys.argv):
                return sys.argv[idx + 1]
        except ValueError:
            pass
        return default

    # S3 bucket and paths - using environment variables
    # Get bucket names from default arguments passed by CDK, with fallback defaults
    source_bucket_name = get_optional_arg("SOURCE_BUCKET", "albanyhealthprocessed-s3bucket-dev")
    target_bucket_name = get_optional_arg("TARGET_BUCKET", "albanyhealthmerged-s3bucket-dev")
    input_path = f"s3://{source_bucket_name}/initial_append"  # Changed from temp_input_path
    output_path = f"s3://{target_bucket_name}/merged_garmin_data"

    log_info("Job initialized successfully")

    # First, list all patient folders in initial_append
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # Function to prepare DataFrames for joining
    def prepare_for_join(df, data_type):
        if df is None:
            return None
            
        # Log columns before prep
        columns_before = df.columns
        log_info(f"Columns before prep for {data_type}: {columns_before}")

        # Check if participantId and timestamp columns exist
        if "participant_id" not in df.columns or "timestamp" not in df.columns:
            if "participantid_timestamp" in df.columns:
                # Extract if they don't exist but composite key does
                df = df.withColumn(
                    "participant_id",
                    split(col("participantid_timestamp"), "_").getItem(0),
                )
                df = df.withColumn(
                    "timestamp", split(col("participantid_timestamp"), "_").getItem(1)
                )
            else:
                log_error(f"ERROR: Required joining columns missing in {data_type} data")
                raise Exception(f"Join columns not found in {data_type} data")

        # Preserve unixTimestampInMs column if it exists
        has_unix_timestamp = "unixTimestampInMs" in df.columns

        # Select columns specific to each data type
        try:
            if data_type == "garmin-device-heart-rate":
                if "beatsPerMinute" in df.columns:
                    select_cols = ["participant_id", "timestamp", "beatsPerMinute"]
                else:
                    heart_cols = [c for c in df.columns if "heart" in c.lower() or "beat" in c.lower() or "bpm" in c.lower()]
                    if heart_cols:
                        heart_col = heart_cols[0]
                        select_cols = ["participant_id", "timestamp"]
                        df = df.withColumn("beatsPerMinute", col(heart_col))
                        select_cols.append("beatsPerMinute")
                    else:
                        select_cols = ["participant_id", "timestamp"]
                        df = df.withColumn("beatsPerMinute", lit(0))
                        select_cols.append("beatsPerMinute")
            elif data_type == "garmin-connect-sleep-stage":
                duration_col = "durationInMs" if "durationInMs" in df.columns else next((c for c in df.columns if "duration" in c.lower()), None)
                type_col = "type" if "type" in df.columns else next((c for c in df.columns if "type" in c.lower() or "stage" in c.lower()), None)

                select_cols = ["participant_id", "timestamp"]

                if duration_col:
                    if duration_col != "durationInMs":
                        df = df.withColumn("durationInMs", col(duration_col))
                    select_cols.append("durationInMs")
                else:
                    df = df.withColumn("durationInMs", lit(0))
                    select_cols.append("durationInMs")

                if type_col:
                    if type_col != "sleepType":
                        df = df.withColumn("sleepType", col(type_col))
                    select_cols.append("sleepType")
                else:
                    df = df.withColumn("sleepType", lit("unknown"))
                    select_cols.append("sleepType")
            elif data_type == "garmin-device-step":
                steps_col = "steps" if "steps" in df.columns else next((c for c in df.columns if "step" in c.lower() and "total" not in c.lower()), None)
                total_steps_col = "totalSteps" if "totalSteps" in df.columns else next((c for c in df.columns if "total" in c.lower() and "step" in c.lower()), None)

                select_cols = ["participant_id", "timestamp"]

                if steps_col:
                    if steps_col != "steps":
                        df = df.withColumn("steps", col(steps_col))
                    select_cols.append("steps")
                else:
                    df = df.withColumn("steps", lit(0))
                    select_cols.append("steps")

                if total_steps_col:
                    if total_steps_col != "totalSteps":
                        df = df.withColumn("totalSteps", col(total_steps_col))
                    select_cols.append("totalSteps")
                else:
                    df = df.withColumn("totalSteps", lit(0))
                    select_cols.append("totalSteps")
            elif data_type == "garmin-device-respiration":
                respiration_col = "breathsPerMinute" if "breathsPerMinute" in df.columns else next((c for c in df.columns if "breath" in c.lower() or "respiration" in c.lower()), None)

                select_cols = ["participant_id", "timestamp"]

                if respiration_col:
                    if respiration_col != "breathsPerMinute":
                        df = df.withColumn("breathsPerMinute", col(respiration_col))
                    select_cols.append("breathsPerMinute")
                else:
                    df = df.withColumn("breathsPerMinute", lit(0))
                    select_cols.append("breathsPerMinute")
            elif data_type == "garmin-device-stress":
                stress_col = "stressLevel" if "stressLevel" in df.columns else next((c for c in df.columns if "stress" in c.lower() or "level" in c.lower()), None)

                select_cols = ["participant_id", "timestamp"]

                if stress_col:
                    if stress_col != "stressLevel":
                        df = df.withColumn("stressLevel", col(stress_col))
                    select_cols.append("stressLevel")
                else:
                    df = df.withColumn("stressLevel", lit(0))
                    select_cols.append("stressLevel")
            elif data_type == "garmin-device-pulse-ox":
                spo2_col = "spo2" if "spo2" in df.columns else next((c for c in df.columns if "spo2" in c.lower() or "oxygen" in c.lower() or "pulse" in c.lower()), None)

                select_cols = ["participant_id", "timestamp"]

                if spo2_col:
                    if spo2_col != "spo2":
                        df = df.withColumn("spo2", col(spo2_col))
                    select_cols.append("spo2")
                else:
                    df = df.withColumn("spo2", lit(0))
                    select_cols.append("spo2")
            else:
                select_cols = ["participant_id", "timestamp"]

            # Add unixTimestampInMs if it exists
            if has_unix_timestamp:
                select_cols.append("unixTimestampInMs")

            # Select the final columns
            prepped_df = df.select(*select_cols)

            # Log columns after prep
            columns_after = prepped_df.columns
            log_info(f"Columns after prep for {data_type}: {columns_after}")

            return prepped_df
        except Exception as e:
            log_error(f"Error preparing {data_type} data for join", str(e))
            # Return basic DataFrame as fallback
            basic_cols = ["participant_id", "timestamp"]
            if has_unix_timestamp:
                basic_cols.append("unixTimestampInMs")
            return df.select(*basic_cols)

    # Function to join metric data
    def join_metric_data(merged_df, data_type, metric_cols):
        if data_type in prepared_dfs:
            try:
                df = prepared_dfs[data_type]
                slim_cols = ["participant_id", "timestamp"] + metric_cols
                df_slim = df.select(*slim_cols)

                merged_df = merged_df.join(df_slim, ["participant_id", "timestamp"], "left")
                
                # Add default values for null columns
                for col_name in metric_cols:
                    if col_name in df.columns:
                        if col_name != "sleepType":
                            merged_df = merged_df.withColumn(
                                col_name,
                                when(col(col_name).isNull(), 0).otherwise(col(col_name)),
                            )
                        else:
                            merged_df = merged_df.withColumn(
                                col_name,
                                when(col(col_name).isNull(), "none").otherwise(col(col_name)),
                            )
                
                log_info(f"{data_type} data joined successfully")
            except Exception as e:
                log_error(f"Error joining {data_type} data", str(e))
                for col_name in metric_cols:
                    if col_name == "sleepType":
                        merged_df = merged_df.withColumn(col_name, lit("none"))
                    else:
                        merged_df = merged_df.withColumn(col_name, lit(0))
        else:
            for col_name in metric_cols:
                if col_name == "sleepType":
                    merged_df = merged_df.withColumn(col_name, lit("none"))
                else:
                    merged_df = merged_df.withColumn(col_name, lit(0))
        
        return merged_df

    try:
        # Get all patient folders
        patient_folders = set()
        prefix = "initial_append/"
        
        for page in paginator.paginate(Bucket=source_bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    # Extract patient ID from path
                    path_parts = obj["Key"].split("/")
                    if len(path_parts) > 2:  # initial_append/patient_id/file.csv
                        patient_folders.add(path_parts[1])
        
        log_info(f"Found {len(patient_folders)} patient folders to process")
        
        # Process each patient's data separately
        for patient_id in patient_folders:
            log_info(f"Processing data for patient: {patient_id}")
            
            # List of data types to process
            data_types = [
                "garmin-device-heart-rate",
                "garmin-connect-sleep-stage",
                "garmin-device-step",
                "garmin-device-respiration",
                "garmin-device-stress",
                "garmin-device-pulse-ox"
            ]

            # Read and prepare each data type for this patient
            prepared_dfs = {}
            for data_type in data_types:
                try:
                    log_info(f"Reading {data_type} data for patient {patient_id}...")
                    input_file = f"{input_path}/{patient_id}/{data_type}.csv"
                    
                    # Read the data
                    df = glueContext.create_dynamic_frame.from_options(
                        format_options={
                            "quoteChar": '"',
                            "withHeader": True,
                            "separator": ",",
                        },
                        connection_type="s3",
                        format="csv",
                        connection_options={"paths": [input_file]},
                        transformation_ctx=f"{patient_id}_{data_type}_df",
                    ).toDF()
                    
                    # Prepare the data for joining
                    prepared_dfs[data_type] = prepare_for_join(df, data_type)
                    if prepared_dfs[data_type] is not None:
                        log_info(f"Prepared {data_type} data with {prepared_dfs[data_type].count()} records for patient {patient_id}")
                except Exception as e:
                    log_error(f"Failed to process {data_type} data for patient {patient_id}", str(e))
                    prepared_dfs[data_type] = None

            # Create a unified table of unique participantId-timestamp combinations
            unique_combinations = []
            for data_type, df in prepared_dfs.items():
                if df is not None:
                    # Create a new DataFrame with explicit column references
                    select_cols = [col("participant_id"), col("timestamp")]
                    if "unixTimestampInMs" in df.columns:
                        select_cols.append(col("unixTimestampInMs").alias(f"{data_type}_unixTimestampInMs"))
                    unique_combinations.append(df.select(*select_cols).distinct())

            if not unique_combinations:
                log_error(f"No valid data to process for patient {patient_id}")
                continue

            # Create a base DataFrame with all unique participantId-timestamp combinations
            base_df = unique_combinations[0]
            for i in range(1, len(unique_combinations)):
                df_to_join = unique_combinations[i]
                join_cols = ["participant_id", "timestamp"]
                base_df = base_df.join(df_to_join, join_cols, "full_outer")

            # Remove duplicates and handle unixTimestampInMs columns
            base_df = base_df.distinct()
            
            # Select the first non-null unixTimestampInMs value from any of the joined columns
            unix_timestamp_cols = [c for c in base_df.columns if c.endswith("_unixTimestampInMs")]
            if unix_timestamp_cols:
                # Create a coalesce expression to get the first non-null value
                coalesce_expr = F.coalesce(*[col(c) for c in unix_timestamp_cols])
                base_df = base_df.select(
                    col("participant_id"),
                    col("timestamp"),
                    coalesce_expr.alias("unixTimestampInMs")
                )
            else:
                base_df = base_df.select("participant_id", "timestamp")
            
            log_info(f"Created base DataFrame with {base_df.count()} unique participantId-timestamp combinations for patient {patient_id}")

            # Now join each dataset with the metrics
            merged_df = base_df

            # Join each data type
            merged_df = join_metric_data(merged_df, "garmin-device-heart-rate", ["beatsPerMinute"])
            merged_df = join_metric_data(merged_df, "garmin-connect-sleep-stage", ["durationInMs", "sleepType"])
            merged_df = join_metric_data(merged_df, "garmin-device-step", ["steps", "totalSteps"])
            merged_df = join_metric_data(merged_df, "garmin-device-respiration", ["breathsPerMinute"])
            merged_df = join_metric_data(merged_df, "garmin-device-stress", ["stressLevel"])
            merged_df = join_metric_data(merged_df, "garmin-device-pulse-ox", ["spo2"])

            # Add participantid_timestamp back for backward compatibility
            merged_df = merged_df.withColumn(
                "participantid_timestamp",
                F.concat(col("participant_id"), lit("_"), col("timestamp")),
            )
            
            # Set proper datatypes for all columns
            merged_df = merged_df.withColumn("timestamp", col("timestamp").cast("string"))
            merged_df = merged_df.withColumn("participant_id", col("participant_id").cast("string"))
            if "unixTimestampInMs" in merged_df.columns:
                merged_df = merged_df.withColumn("unixTimestampInMs", col("unixTimestampInMs").cast("bigint"))
            merged_df = merged_df.withColumn("beatsPerMinute", col("beatsPerMinute").cast("int"))
            merged_df = merged_df.withColumn("stressLevel", col("stressLevel").cast("int"))
            merged_df = merged_df.withColumn("steps", col("steps").cast("int"))
            merged_df = merged_df.withColumn("totalSteps", col("totalSteps").cast("int"))
            merged_df = merged_df.withColumn("breathsPerMinute", col("breathsPerMinute").cast("int"))
            merged_df = merged_df.withColumn("spo2", col("spo2").cast("int"))
            merged_df = merged_df.withColumn("durationInMs", col("durationInMs").cast("bigint"))
            merged_df = merged_df.withColumn("sleepType", col("sleepType").cast("string"))
            merged_df = merged_df.withColumn("participantid_timestamp", col("participantid_timestamp").cast("string"))
            
            # Extract day and time from timestamp
            # Extract day and time from timestamp
            merged_df = merged_df.withColumn(
                "day",
                F.date_format(F.from_unixtime(F.col("unixTimestampInMs")/1000), "yyyy-MM-dd")
            )
            merged_df = merged_df.withColumn(
                "time",
                F.date_format(F.from_unixtime(F.col("unixTimestampInMs")/1000), "HH:mm:ss")
            )
            
            # Add this right after the above code to create the new participantId_date column:
            # Create participantId_date column by concatenating participant_id and day
            merged_df = merged_df.withColumn(
                "participantId_date",
                F.concat(col("participant_id"), lit("_"), col("day"))
            )
            
            # Log that we've added the new column
            log_info("Added participantId_date column")
            
            # Update the column order list to include the new column
            column_order = [
                "participantid_timestamp",  # Composite key first
                "participantId_date",       # New column we're adding
                "participant_id",            # Primary identifiers
                "timestamp",
                "unixTimestampInMs",        # Time-related fields
                "day",                      # New day column
                "time",                     # New time column
                "beatsPerMinute",           # Health metrics
                "stressLevel",
                "steps",
                "totalSteps",
                "breathsPerMinute",
                "spo2",
                "durationInMs",             # Sleep-related fields
                "sleepType"
            ]
            
            # Select columns in the specified order
            missing_columns = [col for col in column_order if col not in merged_df.columns]
            if missing_columns:
                log_info(f"Warning: Some columns in the desired order are missing: {missing_columns}")
                column_order = [col for col in column_order if col in merged_df.columns]
            
            extra_columns = [col for col in merged_df.columns if col not in column_order]
            if extra_columns:
                log_info(f"Found additional columns not in specified order: {extra_columns}")
                column_order.extend(extra_columns)
            
            # Reorder the columns
            merged_df = merged_df.select(*column_order)
            
            # Log final schema
            final_columns = merged_df.columns
            log_info(f"Final columns in merged data after reordering: {final_columns}")
            log_info(f"Final record count: {merged_df.count()}")

            # Order by unixTimestampInMs before writing
            try:
                log_info("Ordering data by unixTimestampInMs in ascending order")
                if "unixTimestampInMs" in merged_df.columns:
                    if merged_df.schema["unixTimestampInMs"].dataType.typeName() != "bigint":
                        merged_df = merged_df.withColumn(
                            "unixTimestampInMs", col("unixTimestampInMs").cast("bigint")
                        )
                    merged_df = merged_df.orderBy("unixTimestampInMs")
                    log_info("Data successfully ordered by unixTimestampInMs")
                else:
                    log_error("Cannot order by unixTimestampInMs: column not found")
            except Exception as e:
                log_error(f"Error ordering data by unixTimestampInMs: {str(e)}")
                log_info("Will continue with unordered data")

            # Convert to dynamic frame for writing
            try:
                log_info("Converting to dynamic frame for output")
                merged_dynamic_frame = DynamicFrame.fromDF(
                    merged_df, glueContext, "merged_data"
                )
                log_info("Conversion successful")
            except Exception as e:
                log_error("Error creating dynamic frame", str(e))
                log_info("Attempting to save DataFrame directly")
                output_final_path = f"{output_path}/merged_garmin_data_backup"
                merged_df.write.csv(output_final_path, header=True, mode="overwrite")
                log_info(f"Saved backup data to {output_final_path}")
                raise e

            # Write output for this patient
            try:
                log_info(f"Writing merged data for patient {patient_id}")
                
                # Find the latest date from the 'day' column
                if "day" in merged_df.columns:
                    latest_date_df = merged_df.select(F.max("day").alias("latest_date"))
                    latest_date = latest_date_df.collect()[0]["latest_date"]
                    
                    if latest_date is not None:
                        # Convert from yyyy-MM-dd to yyyyMMdd format
                        date_str = latest_date.replace("-", "")
                        log_info(f"Using latest date from data: {date_str}")
                    else:
                        date_str = datetime.datetime.now().strftime("%Y%m%d")
                        log_info(f"No date found, using current date: {date_str}")
                else:
                    date_str = datetime.datetime.now().strftime("%Y%m%d")
                    log_info(f"No day column found, using current date: {date_str}")
                
                # Create patient-specific output folder
                output_folder = f"{output_path}/{patient_id}"
                output_filename = f"merged_file_{date_str}.csv"
                
                # Create a temporary folder for our part file
                temp_folder = f"{output_path}/temp_merged_data_{patient_id}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
                
                # Coalesce to 1 partition to create a single file
                single_partition_df = merged_df.coalesce(1)
                
                # Write as a single CSV file to the temp folder
                log_info(f"Writing merged data to temporary location {temp_folder}")
                single_partition_df.write.option("header", "true").option("quote", '"').option(
                    "sep", ","
                ).mode("overwrite").csv(temp_folder)
                
                # Find the part file in the temp folder
                temp_key_prefix = temp_folder.replace(f"s3://{target_bucket_name}/", "")
                response = s3_client.list_objects_v2(Bucket=target_bucket_name, Prefix=temp_key_prefix)
                
                # Find the actual CSV file
                part_file = None
                if "Contents" in response:
                    for obj in response["Contents"]:
                        if obj["Key"].endswith(".csv") and "part-" in obj["Key"]:
                            part_file = obj["Key"]
                            break
                
                if part_file:
                    # Copy to final destination
                    target_key = f"{output_folder.replace(f's3://{target_bucket_name}/', '')}/{output_filename}"
                    
                    # Check if file already exists
                    try:
                        s3_client.head_object(Bucket=target_bucket_name, Key=target_key)
                        # If file exists, add timestamp to filename
                        current_time = datetime.datetime.now().strftime("%H%M%S")
                        output_filename = f"merged_file_{date_str}_{current_time}.csv"
                        target_key = f"{output_folder.replace(f's3://{target_bucket_name}/', '')}/{output_filename}"
                        log_info(f"File already exists, using new name: {output_filename}")
                    except:
                        # File doesn't exist, continue with original name
                        pass
                    
                    s3_client.copy_object(
                        Bucket=target_bucket_name,
                        CopySource={"Bucket": target_bucket_name, "Key": part_file},
                        Key=target_key
                    )
                    
                    # Clean up temp files
                    for obj in response["Contents"]:
                        s3_client.delete_object(Bucket=target_bucket_name, Key=obj["Key"])
                    
                    log_info(f"Successfully saved merged data for patient {patient_id} as {output_filename}")
                    log_info(f"Total records for patient {patient_id}: {merged_df.count()}")
                else:
                    log_error(f"Could not find temporary part file for patient {patient_id}")
                    
            except Exception as e:
                log_error(f"Error writing output for patient {patient_id}: {str(e)}")
                continue

        log_info("Completed processing all patients")
        
    except Exception as e:
        log_error("Error listing patient folders", str(e))
        raise e

    job.commit()
    log_info("Job completed successfully")

except Exception as e:
    log_error("Fatal error in main job flow", str(e))
    raise e 