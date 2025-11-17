import sys
import datetime
import traceback
import re
import time
import os
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    lit, col, lag, avg, stddev, sqrt, 
    pow, abs as spark_abs, udf, max as spark_max  # Added spark_max for finding latest date
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.window import Window
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize error logging
def log_error(message, error_details=None):
    """Log errors to console"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_message = f"[{timestamp}] ERROR: {message}\n"
    if error_details:
        log_message += f"Details: {error_details}\n"
        log_message += f"Traceback: {traceback.format_exc()}\n"
    
    print(log_message)

# Simple info logging to console
def log_info(message):
    """Log information to console"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"[{timestamp}] INFO: {message}")

# Extract participant ID from file path
def extract_participant_id(file_path):
    """Extract participant ID from file path"""
    match = re.search(r'_([a-z0-9]{8})\.csv$', file_path)
    return match.group(1) if match else None

# Format ISO timestamp to yyyy-mm-dd only
def format_iso_timestamp(iso_date_string):
    """Format ISO date string to yyyy-mm-dd format"""
    if not iso_date_string or not isinstance(iso_date_string, str):
        return None
    
    try:
        # Handle different datetime formats
        iso_string = str(iso_date_string).strip()
        
        # Replace Z with +00:00 for proper ISO parsing
        iso_string = iso_string.replace('Z', '+00:00')
        
        # Try different parsing approaches
        try:
            # First try standard ISO format
            dt = datetime.datetime.fromisoformat(iso_string)
        except:
            try:
                # Try parsing with space separator (like "2025-06-07 17:14:22.856000")
                if ' ' in iso_string:
                    date_part = iso_string.split(' ')[0]
                    return date_part  # Already in yyyy-mm-dd format
                else:
                    # Try parsing as standard datetime
                    dt = datetime.datetime.strptime(iso_string, '%Y-%m-%d %H:%M:%S.%f')
            except:
                try:
                    # Try without microseconds
                    dt = datetime.datetime.strptime(iso_string, '%Y-%m-%d %H:%M:%S')
                except:
                    # If all parsing fails, try to extract just the date part
                    if len(iso_string) >= 10 and iso_string[4] == '-' and iso_string[7] == '-':
                        return iso_string[:10]  # Extract first 10 characters (yyyy-mm-dd)
                    return None
        
        return dt.strftime('%Y-%m-%d') 
    except Exception as e:
        print(f"Error formatting ISO date '{iso_date_string}': {str(e)}")
        # Try to extract date part as fallback
        try:
            iso_string = str(iso_date_string).strip()
            if len(iso_string) >= 10 and iso_string[4] == '-' and iso_string[7] == '-':
                return iso_string[:10]  # Extract first 10 characters (yyyy-mm-dd)
        except:
            pass
    return None

# Create UDF for ISO date transformation
def create_iso_date_udf():
    """Create a UDF for transforming ISO dates to date-only format"""
    return udf(format_iso_timestamp, StringType())

# Calculate HRV metrics for a dataframe
def calculate_hrv_metrics(df, participant_id):
    """
    Calculate Heart Rate Variability metrics for a given dataframe
    Returns a new dataframe with summarized metrics in a single row, preserving original columns
    except for bbi, bbi_diff, and bbi_diff_squared which are eliminated
    """
    try:
        log_info(f"Calculating HRV metrics for participant: {participant_id}")
        
        # Ensure we have the necessary columns
        required_columns = ["bbi", "unixTimestampInMs"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            log_info(f"File for {participant_id} is missing required columns: {missing_columns}")
            # Create a simple dataframe with zeros for metrics
            return df.limit(1).withColumn("participantId_date", lit(f"{participant_id}_unknown")) \
                             .withColumn("sdrr", lit(0.0)) \
                             .withColumn("rmssd", lit(0.0)) \
                             .withColumn("rr50", lit(0)) \
                             .withColumn("prr50", lit(0.0)) \
                             .withColumn("mean_bbi", lit(0.0)) \
                             .withColumn("heart_rate", lit(0.0)) \
                             .withColumn("record_count", lit(df.count()))
            
        # Store original columns to preserve (excluding bbi, bbi_diff, bbi_diff_squared and unixTimestampInMs)
        original_columns = [col_name for col_name in df.columns 
                           if col_name not in [
                               "bbi", "bbi_diff", "bbi_diff_squared", "prev_bbi", 
                               "diff", "window_5min", "timestamp_seconds", "unixTimestampInMs"
                           ]]
        
        # Get the timestamp range for this file
        start_time = df.select("unixTimestampInMs").agg({"unixTimestampInMs": "min"}).collect()[0][0]
        
        # Calculate a formatted date for the file
        start_datetime = datetime.datetime.fromtimestamp(start_time / 1000)  # Convert ms to seconds
        formatted_date = start_datetime.strftime("%Y-%m-%d")
        
        # Convert to numeric for calculations
        df = df.withColumn("bbi", col("bbi").cast("double"))
        
        # Calculate Mean BBI and Heart Rate
        try:
            mean_bbi_result = df.select(avg("bbi").alias("mean_bbi")).collect()
            mean_bbi = mean_bbi_result[0]["mean_bbi"] if mean_bbi_result and mean_bbi_result[0]["mean_bbi"] is not None else 0.0
            
            # Round to 4 decimal places
            mean_bbi = round(mean_bbi, 4)
            
            # Convert mean BBI (in ms) to heart rate (beats per minute)
            # HR = 60,000 / mean BBI (when BBI is in ms)
            heart_rate = 60000.0 / mean_bbi if mean_bbi > 0 else 0.0
            
            # Round to 4 decimal places
            heart_rate = round(heart_rate, 4)
        except Exception as e:
            log_error(f"Error calculating Mean BBI and Heart Rate: {str(e)}")
            mean_bbi = 0.0
            heart_rate = 0.0
        
        # 1. Calculate SDRR (Standard Deviation of R-R intervals) - same as SDNN
        try:
            sdrr_result = df.select(stddev("bbi").alias("sdrr")).collect()
            sdrr = sdrr_result[0]["sdrr"] if sdrr_result and sdrr_result[0]["sdrr"] is not None else 0.0
            
            # Round to 4 decimal places
            sdrr = round(sdrr, 4)
        except Exception as e:
            log_error(f"Error calculating SDRR: {str(e)}")
            sdrr = 0.0
        
        # 2. Calculate RMSSD (Root Mean Square of Successive Differences)
        try:
            # We already have bbi_diff from the previous processing
            if "bbi_diff" in df.columns:
                # RMSSD calculation using existing bbi_diff
                rmssd_result = df.select(
                    sqrt(avg(pow(col("bbi_diff"), 2))).alias("rmssd")
                ).collect()
                rmssd = rmssd_result[0]["rmssd"] if rmssd_result and rmssd_result[0]["rmssd"] is not None else 0.0
            else:
                # Calculate RMSSD from scratch
                window_spec = Window.orderBy("unixTimestampInMs")
                df = df.withColumn("prev_bbi", lag("bbi", 1).over(window_spec))
                df = df.withColumn("diff", col("bbi") - col("prev_bbi"))
                diff_df = df.filter(col("diff").isNotNull())
                if diff_df.count() > 0:
                    rmssd_result = diff_df.select(
                        sqrt(avg(pow(col("diff"), 2))).alias("rmssd")
                    ).collect()
                    rmssd = rmssd_result[0]["rmssd"] if rmssd_result and rmssd_result[0]["rmssd"] is not None else 0.0
                else:
                    rmssd = 0.0
            
            # Round to 4 decimal places
            rmssd = round(rmssd, 4)
        except Exception as e:
            log_error(f"Error calculating RMSSD: {str(e)}")
            rmssd = 0.0
        
        # 3. Calculate RR50 (Count of intervals >50ms) and pRR50 (Percentage)
        try:
            if "bbi_diff" in df.columns:
                # Use existing bbi_diff
                diff_column = "bbi_diff"
            elif "diff" in df.columns:
                # Use the diff column we created for RMSSD
                diff_column = "diff"
            else:
                # Create the diff column
                window_spec = Window.orderBy("unixTimestampInMs")
                df = df.withColumn("diff", spark_abs(col("bbi") - lag("bbi", 1).over(window_spec)))
                diff_column = "diff"
            
            # Count intervals > 50ms
            diff_df = df.filter(col(diff_column).isNotNull())
            if diff_df.count() > 0:
                total_intervals = diff_df.count()
                rr50 = diff_df.filter(col(diff_column) > 50).count()
                prr50 = (rr50 / total_intervals) * 100 if total_intervals > 0 else 0.0
                
                # Round to 4 decimal places
                prr50 = round(prr50, 4)
            else:
                rr50 = 0
                prr50 = 0.0
        except Exception as e:
            log_error(f"Error calculating RR50 and pRR50: {str(e)}")
            rr50 = 0
            prr50 = 0.0
        
        # Create the participantId_date value by combining participant_id and formatted_date
        participantId_date = f"{participant_id}_{formatted_date}"
        
        # Get sample values for all columns we want to preserve
        # We'll use the first row's values for most columns
        if df.count() > 0:
            sample_row = df.select(original_columns).limit(1).collect()[0] if original_columns else {}
            
            # Build the column list for our result dataframe
            # First with the new participantId_date column, then the original columns we're keeping
            result_columns = ["participantId_date"]  # Add our new first column
            result_values = [participantId_date]     # Add the value for our new column
            
            # Add the original columns from the first row
            for col_name in original_columns:
                result_columns.append(col_name)
                result_values.append(sample_row[col_name])
            
            # Add our new calculated metrics columns (removed "date" column)
            result_columns.extend(["sdrr", "rmssd", "rr50", "prr50", "mean_bbi", "heart_rate", "record_count"])
            result_values.extend([sdrr, rmssd, rr50, prr50, mean_bbi, heart_rate, df.count()])
            
            # Create a schema dynamically based on the columns
            schema_fields = []
            for i, col_name in enumerate(result_columns):
                # Determine the type based on the value
                value = result_values[i]
                if isinstance(value, str):
                    field_type = StringType()
                elif isinstance(value, int):
                    field_type = LongType()
                elif isinstance(value, float):
                    field_type = DoubleType()
                else:
                    # Default to string for other types
                    field_type = StringType()
                    result_values[i] = str(result_values[i])
                
                schema_fields.append(StructField(col_name, field_type, True))
            
            schema = StructType(schema_fields)
            
            # Create the metrics row
            results_df = spark.createDataFrame([tuple(result_values)], schema)
        else:
            # Create a simple dataframe with zeros for metrics, adding the participantId_date column first (removed "date" column)
            results_df = df.limit(1).withColumn("participantId_date", lit(participantId_date)) \
                                   .withColumn("sdrr", lit(sdrr)) \
                                   .withColumn("rmssd", lit(rmssd)) \
                                   .withColumn("rr50", lit(rr50)) \
                                   .withColumn("prr50", lit(prr50)) \
                                   .withColumn("mean_bbi", lit(mean_bbi)) \
                                   .withColumn("heart_rate", lit(heart_rate)) \
                                   .withColumn("record_count", lit(df.count()))
        
        log_info(f"HRV metrics calculated for participant {participant_id}: SDRR={sdrr}, RMSSD={rmssd}, RR50={rr50}, pRR50={prr50}%, Mean BBI={mean_bbi}, HR={heart_rate}")
        
        return results_df
        
    except Exception as e:
        log_error(f"Error calculating HRV metrics: {str(e)}")
        # Create a simple dataframe with zeros for metrics and an error column, with participantId_date first (removed "date" column)
        return df.limit(1).withColumn("participantId_date", lit(f"{participant_id}_{datetime.datetime.now().strftime('%Y-%m-%d')}")) \
                         .withColumn("sdrr", lit(0.0)) \
                         .withColumn("rmssd", lit(0.0)) \
                         .withColumn("rr50", lit(0)) \
                         .withColumn("prr50", lit(0.0)) \
                         .withColumn("mean_bbi", lit(0.0)) \
                         .withColumn("heart_rate", lit(0.0)) \
                         .withColumn("error", lit(str(e))) \
                         .withColumn("record_count", lit(df.count()))

def transform_isodate_column(df):
    """Transform the isodate column to extract just the date part (yyyy-mm-dd)"""
    try:
        # Check for different possible column names (case-insensitive)
        column_names = df.columns
        isodate_column = None
        
        # Look for isodate column with different case variations
        for col_name in column_names:
            if col_name.lower() in ['isodate', 'iso_date', 'isoDate']:
                isodate_column = col_name
                break
        
        if isodate_column:
            log_info(f"Found isodate column: '{isodate_column}'. Transforming to date-only format")
            
            # Create UDF for ISO date transformation
            iso_date_udf = create_iso_date_udf()
            
            # Transform the isodate column
            df = df.withColumn(isodate_column, iso_date_udf(col(isodate_column)))
            
            log_info(f"Successfully transformed {isodate_column} column")
        else:
            log_info(f"No isodate column found. Available columns: {column_names}")
            
        return df
        
    except Exception as e:
        log_error(f"Error transforming isodate column: {str(e)}")
        return df

def get_latest_iso_date(df):
    """Extract the latest ISO date from the dataframe for use in filename"""
    try:
        # Check for different possible column names (case-insensitive)
        column_names = df.columns
        isodate_column = None
        
        # Look for isodate column with different case variations
        for col_name in column_names:
            if col_name.lower() in ['isodate', 'iso_date', 'isoDate']:
                isodate_column = col_name
                break
        
        if isodate_column:
            # Get the maximum (latest) date from the isodate column
            latest_date_result = df.select(spark_max(col(isodate_column)).alias("latest_date")).collect()
            latest_date = latest_date_result[0]["latest_date"] if latest_date_result and latest_date_result[0]["latest_date"] is not None else None
            
            if latest_date:
                # Format the date if it's not already in the right format
                formatted_date = format_iso_timestamp(str(latest_date))
                return formatted_date if formatted_date else latest_date
        
        # Fallback to current date if no isodate column found
        return datetime.datetime.now().strftime("%Y-%m-%d")
        
    except Exception as e:
        log_error(f"Error getting latest ISO date: {str(e)}")
        return datetime.datetime.now().strftime("%Y-%m-%d")

try:
    # Track job start time
    job_start_time = time.time()
    
    # Get required parameters from job arguments
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job_name = args["JOB_NAME"]
    
    # Set bucket names
    source_bucket_name = "albanyhealthbbiprocessingbucket"
    destination_bucket_name = "albanyhealthbbimergedbucket"
    
    # Override with parameters if provided
    if "source_bucket" in args:
        source_bucket_name = args["source_bucket"]
    if "destination_bucket" in args:
        destination_bucket_name = args["destination_bucket"]
    
    # Initialize Spark context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    
    # Configure Spark for better performance
    spark.conf.set("spark.sql.shuffle.partitions", "100")
    spark.conf.set("spark.default.parallelism", "100")
    
    # Initialize job
    job.init(job_name, args)
    
    log_info(f"Job initialized - Source: {source_bucket_name}, Destination: {destination_bucket_name}")
    
    # List S3 files
    import boto3
    s3_client = boto3.client("s3")
    
    # Find all processed BBI files in the source bucket
    log_info(f"Searching for processed BBI files in {source_bucket_name}...")
    
    bbi_files = []
    
    # Search for all CSV files in the bucket
    paginator = s3_client.get_paginator("list_objects_v2")
    
    for page in paginator.paginate(Bucket=source_bucket_name):
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                if key.endswith(".csv"):
                    bbi_files.append(key)
    
    log_info(f"Found {len(bbi_files)} BBI files for processing")
    
    if len(bbi_files) == 0:
        log_error("No BBI files found in bucket: " + source_bucket_name)
        raise ValueError("No BBI files found in the source bucket")
    
    # Log sample of files
    if bbi_files:
        sample_size = min(5, len(bbi_files))
        log_info(f"Sample BBI files: {bbi_files[:sample_size]}")
    
    # Process each BBI file
    processed_files = []
    failed_files = []
    total_records_processed = 0
    
    # Keep track of files by participant folder path for merging later
    participant_files = {}
    
    # Create output directory if it doesn't exist
    try:
        log_info(f"Ensuring destination bucket is accessible")
        s3_client.list_objects_v2(Bucket=destination_bucket_name, MaxKeys=1)
    except Exception as e:
        log_error(f"Error accessing destination bucket: {str(e)}")
        raise ValueError(f"Cannot access destination bucket: {destination_bucket_name}")
    
    # Process each file
    for file_path in bbi_files:
        try:
            s3_path = f"s3://{source_bucket_name}/{file_path}"
            log_info(f"Processing file: {s3_path}")
            
            # Extract participant ID from file name
            participant_id = extract_participant_id(file_path)
            if not participant_id:
                log_info(f"Could not extract participant ID from {file_path}, using filename as identifier")
                participant_id = os.path.basename(file_path).replace(".csv", "")
            
            # Extract the full folder path structure (everything except the filename)
            folder_path = os.path.dirname(file_path)
            
            # Read the processed BBI file
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)
            
            # Check if the file has data
            record_count = df.count()
            if record_count == 0:
                log_info(f"No data found in {file_path}")
                failed_files.append({"file": file_path, "error": "File contains no data"})
                continue
                
            log_info(f"Loaded {record_count} records with columns: {df.columns}")
            
            # Calculate HRV metrics
            metrics_df = calculate_hrv_metrics(df, participant_id)
            
            if metrics_df is None:
                log_error(f"Failed to calculate metrics for {file_path}")
                failed_files.append({"file": file_path, "error": "Metrics calculation failed"})
                continue
            
            # Store the calculated metrics dataframe in memory for merging later
            log_info(f"Successfully processed metrics for: {file_path}")
            processed_files.append(file_path)
            total_records_processed += record_count
            
            # Store the metrics dataframe for merging later - group by folder path
            if folder_path not in participant_files:
                participant_files[folder_path] = []
                
            participant_files[folder_path].append({
                "participant_id": participant_id,
                "metrics_df": metrics_df
            })
                
        except Exception as e:
            error_msg = f"Error processing file {file_path}: {str(e)}"
            log_error(error_msg)
            failed_files.append({"file": file_path, "error": str(e)})
    
    # Create merged folder directory (renamed from merged_bbi_folder to merged_folder)
    try:
        s3_client.put_object(
            Bucket=destination_bucket_name,
            Key="merged_folder/",
            Body=''
        )
        log_info("Created merged_folder directory in destination bucket")
    except Exception as e:
        log_info(f"Note: {str(e)}")
    
    # Now merge files by folder path
    log_info(f"Beginning to merge files by folder path...")
    merged_count = 0
    
    for folder_path, files_info in participant_files.items():
        try:
            if not files_info:
                continue
                
            log_info(f"Merging {len(files_info)} files for folder: {folder_path}")
            
            # Create the output directory structure maintaining the same folder structure
            # The output structure will be: merged_folder/{original_folder_structure}/
            merged_output_dir = f"merged_folder/{folder_path}/" if folder_path else "merged_folder/"
            
            # Ensure the directory exists
            s3_client.put_object(
                Bucket=destination_bucket_name,
                Key=merged_output_dir,
                Body=''
            )
            
            # Get all the metrics dataframes for this folder
            all_dfs = [file_info["metrics_df"] for file_info in files_info]
            
            if not all_dfs:
                log_info(f"No valid files to merge for folder {folder_path}")
                continue
                
            # Union all dataframes
            merged_df = all_dfs[0]
            for df in all_dfs[1:]:
                merged_df = merged_df.union(df)
            
            # TRANSFORM THE ISODATE COLUMN AFTER MERGE
            merged_df = transform_isodate_column(merged_df)
            
            # Get the latest ISO date for filename
            latest_iso_date = get_latest_iso_date(merged_df)
            
            # Write the merged file with new naming convention
            merged_output_file = f"{merged_output_dir}merged_{latest_iso_date}.csv"
            
            # Convert to pandas and write locally
            merged_pandas_df = merged_df.toPandas()
            local_merged_file = f"/tmp/merged_{latest_iso_date}_{int(time.time())}.csv"  # Add timestamp to avoid conflicts
            merged_pandas_df.to_csv(local_merged_file, index=False)
            
            # Upload to S3
            s3_client.upload_file(
                local_merged_file, 
                destination_bucket_name, 
                merged_output_file
            )
            
            # Clean up local file
            if os.path.exists(local_merged_file):
                os.remove(local_merged_file)
                
            log_info(f"Successfully created merged file: {merged_output_file}")
            merged_count += 1
            
        except Exception as e:
            log_error(f"Error merging files for folder {folder_path}: {str(e)}")
    
    # Calculate job duration
    job_duration = round((time.time() - job_start_time), 2)
    
    # Log execution summary
    log_info("Glue Job Execution Summary:")
    log_info("-" * 50)
    log_info(f"Source Bucket: {source_bucket_name}")
    log_info(f"Destination Bucket: {destination_bucket_name}")
    log_info(f"Total Files Found: {len(bbi_files)}")
    log_info(f"Total Files Processed: {len(processed_files)}")
    log_info(f"Total Files Failed: {len(failed_files)}")
    log_info(f"Total Folder Groups Merged: {merged_count}")
    log_info(f"Total Records Processed: {total_records_processed}")
    log_info(f"Job Duration: {job_duration} seconds")
    
    if processed_files:
        log_info("\nSuccessfully Processed Files:")
        for file in processed_files[:5]:  # Show first 5
            log_info(f"✓ {file}")
        if len(processed_files) > 5:
            log_info(f"... and {len(processed_files) - 5} more files")
    
    if failed_files:
        log_info("\nFailed Files:")
        for file_info in failed_files[:5]:  # Show first 5
            log_info(f"✗ {file_info['file']}")
            log_info(f"  Error: {file_info['error']}")
        if len(failed_files) > 5:
            log_info(f"... and {len(failed_files) - 5} more files")
    
    log_info("-" * 50)
    
    # Commit the job
    job.commit()
    log_info("Job completed successfully")

except Exception as e:
    log_error("Fatal error in main job flow", str(e))
    raise e