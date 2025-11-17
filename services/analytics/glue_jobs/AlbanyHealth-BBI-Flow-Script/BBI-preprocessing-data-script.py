import sys
import datetime
import traceback
import re
import time
import os
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, col, when, pow, lag
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

# Format ISO timestamp
def format_iso_timestamp(iso_date_string):
    """Format ISO date string to yyyy-mm-dd hh:mm format"""
    if not iso_date_string or not isinstance(iso_date_string, str):
        return None
    
    try:
        # Replace Z with +00:00 for proper ISO parsing
        iso_string = iso_date_string.replace('Z', '+00:00')
        # Parse the ISO string and format it
        dt = datetime.datetime.fromisoformat(iso_string)
        return dt.strftime('%Y-%m-%d %H:%M')
    except Exception as e:
        print(f"Error formatting ISO date: {str(e)}")
    return None

try:
    # Track job start time
    job_start_time = time.time()
    
    # Get required parameters from job arguments
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job_name = args["JOB_NAME"]
    
    # Set default bucket names for Albany Health
    source_bucket_name = "albanyhealthsources3bucket-dev"
    destination_bucket_name = "albanyhealthbbiprocessingbucket"
    
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
    
    # Initialize job
    job.init(job_name, args)
    
    log_info(f"Job initialized - Source: {source_bucket_name}, Destination: {destination_bucket_name}")
    
    # List S3 files
    import boto3
    s3_client = boto3.client("s3")
    
    # Find all garmin-device-bbi folders and their contents
    log_info(f"Searching for garmin-device-bbi folders in {source_bucket_name}...")
    
    bbi_folders = []
    bbi_files = []
    
    # First, find all garmin-device-bbi folders
    paginator = s3_client.get_paginator("list_objects_v2")
    
    for page in paginator.paginate(Bucket=source_bucket_name, Delimiter='/'):
        # Look for top-level prefixes/folders
        if "CommonPrefixes" in page:
            for prefix in page["CommonPrefixes"]:
                prefix_path = prefix["Prefix"]
                
                # Check if this is a garmin-device-bbi folder or might contain one
                if "garmin-device-bbi" in prefix_path.lower():
                    bbi_folders.append(prefix_path)
                
                # Recursively search for garmin-device-bbi folders in this prefix
                sub_paginator = s3_client.get_paginator("list_objects_v2")
                for sub_page in sub_paginator.paginate(Bucket=source_bucket_name, Prefix=prefix_path, Delimiter='/'):
                    if "CommonPrefixes" in sub_page:
                        for sub_prefix in sub_page["CommonPrefixes"]:
                            sub_path = sub_prefix["Prefix"]
                            if "garmin-device-bbi" in sub_path.lower():
                                bbi_folders.append(sub_path)
    
    log_info(f"Found {len(bbi_folders)} garmin-device-bbi folder paths")
    
    # If no specific folders are found, check the entire bucket for files with 'bbi' in the path
    if len(bbi_folders) == 0:
        log_info("No specific garmin-device-bbi folders found, searching entire bucket for BBI files...")
        for page in paginator.paginate(Bucket=source_bucket_name):
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if "bbi" in key.lower() and key.endswith((".csv", ".csv.gz")):
                        bbi_files.append(key)
    else:
        # Now get all CSV files from these folders and subfolders
        for folder in bbi_folders:
            log_info(f"Looking for CSV files in folder: {folder}")
            for page in paginator.paginate(Bucket=source_bucket_name, Prefix=folder):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        if key.endswith((".csv", ".csv.gz")):
                            bbi_files.append(key)
    
    log_info(f"Found {len(bbi_files)} BBI files")
    
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
    
    # Create a temporary S3 folder for intermediate files
    temp_s3_prefix = f"tmp/glue-job-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}/"
    log_info(f"Creating temporary S3 folder: s3://{source_bucket_name}/{temp_s3_prefix}")
    
    # Create the temp folder
    try:
        s3_client.put_object(Bucket=source_bucket_name, Key=temp_s3_prefix, Body='')
        log_info(f"Temporary S3 folder created successfully")
    except Exception as e:
        log_error(f"Error creating temporary S3 folder: {str(e)}")
        # Continue anyway, we'll try to create files in this folder
    
    # Define a function to process each BBI file
    def process_bbi_file(file_path):
        try:
            s3_path = f"s3://{source_bucket_name}/{file_path}"
            log_info(f"Processing file: {s3_path}")
            
            # Extract participant ID from file name
            participant_id = extract_participant_id(file_path)
            if not participant_id:
                log_info(f"Could not extract participant ID from {file_path}, using fallbacks")
            
            # Download the file and process it line by line
            response = s3_client.get_object(Bucket=source_bucket_name, Key=file_path)
            file_content = response['Body'].read().decode('utf-8')
            
            # Split into lines
            lines = file_content.splitlines()
            
            # Look for specific patterns in the file to determine the structure
            header_line = None
            real_data = []
            
            # Try to find the line with timezone and unixTimestampInMs header
            for i, line in enumerate(lines):
                if "timezone,unixTimestampInMs" in line:
                    header_line = line
                    # The next lines should be the real data
                    real_data = lines[i+1:]
                    break
            
            # Generate a unique name for the intermediate S3 file
            file_basename = os.path.basename(file_path)
            temp_s3_key = f"{temp_s3_prefix}{file_basename}"
            temp_s3_path = f"s3://{source_bucket_name}/{temp_s3_key}"
            
            log_info(f"Preparing to write intermediate file to: {temp_s3_path}")
            
            # Prepare the CSV content
            csv_content = ""
            if header_line:
                csv_content = header_line + "\n" + "\n".join(real_data)
                log_info(f"Found proper header. Created CSV with header: {header_line}")
            else:
                # If we couldn't find the header line, try to create a basic CSV structure
                # Look at pairs of lines - timezone followed by timestamp 
                timezone_idx = []
                for i, line in enumerate(lines):
                    if "America/New_York" in line:
                        timezone_idx.append(i)
                
                if timezone_idx:
                    csv_content = "timezone,unixTimestampInMs\n"
                    log_info(f"Created custom header with {len(timezone_idx)} timezone entries")
                    for idx in timezone_idx:
                        if idx + 1 < len(lines):  # Ensure next line exists
                            csv_content += f"{lines[idx].strip()},{lines[idx+1].strip()}\n"
                else:
                    log_info("Could not find timezone entries, attempting to create minimal CSV")
                    csv_content = "data\n" + "\n".join(lines)
            
            # Write the processed content to S3
            try:
                log_info(f"Writing intermediate CSV to S3 ({len(csv_content)} bytes)")
                s3_client.put_object(
                    Bucket=source_bucket_name,
                    Key=temp_s3_key,
                    Body=csv_content
                )
                log_info(f"Successfully wrote intermediate file to S3")
                
                # Verify the file exists
                try:
                    head_response = s3_client.head_object(Bucket=source_bucket_name, Key=temp_s3_key)
                    log_info(f"Verified S3 file exists. Size: {head_response.get('ContentLength', 'unknown')} bytes")
                except Exception as e:
                    log_error(f"Error verifying intermediate S3 file: {str(e)}")
                    return False, f"Error verifying file in S3: {str(e)}", 0
                
            except Exception as e:
                log_error(f"Error writing to S3: {str(e)}")
                return False, f"Error writing to S3: {str(e)}", 0
            
            # Now read the data from S3 using Spark
            log_info(f"Reading intermediate file with Spark from: {temp_s3_path}")
            try:
                df = spark.read.option("header", "true").option("inferSchema", "true").csv(temp_s3_path)
                
                # Check if DataFrame is empty
                record_count = df.count()
                log_info(f"Initial DataFrame loaded with {record_count} rows and columns: {df.columns}")
                
                if record_count == 0:
                    log_info(f"No data found in {file_path} after preprocessing")
                    
                    # Clean up the temporary S3 file
                    try:
                        s3_client.delete_object(Bucket=source_bucket_name, Key=temp_s3_key)
                        log_info(f"Deleted empty intermediate S3 file")
                    except Exception as e:
                        log_info(f"Note: Could not delete intermediate S3 file: {str(e)}")
                    
                    return False, f"No data found in {file_path}", 0
                
                # Add participant ID
                if participant_id:
                    df = df.withColumn("participant_id", lit(participant_id))
                else:
                    df = df.withColumn("participant_id", lit("unknown"))
                
                # Calculate BBI values from timestamps if needed
                if "bbi" not in df.columns and "unixTimestampInMs" in df.columns:
                    window_spec = Window.orderBy("unixTimestampInMs")
                    df = df.withColumn("bbi", 
                                      when(lag("unixTimestampInMs", 1).over(window_spec).isNull(), lit(None))
                                      .otherwise(col("unixTimestampInMs") - lag("unixTimestampInMs", 1).over(window_spec)))
                    
                    # Remove first row where BBI is null (can't calculate difference)
                    df = df.filter(col("bbi").isNotNull())
                
                # Add BBI difference calculations
                if "bbi" in df.columns:
                    # Ensure bbi is numeric
                    df = df.withColumn("bbi", col("bbi").cast("double"))
                    
                    # Define a window specification for calculations
                    window_spec = Window.partitionBy("participant_id").orderBy("unixTimestampInMs")
                    
                    # First create a column with the next row's bbi value using lag with a negative offset
                    # Note: lag with negative offset is equivalent to lead with positive offset
                    df = df.withColumn("next_bbi", lag("bbi", -1).over(window_spec))
                    
                    # Calculate bbi_diff as the absolute difference between current and next
                    df = df.withColumn("bbi_diff", 
                                     when(col("next_bbi").isNull(), lit(0.0))
                                     .otherwise(col("bbi") - col("next_bbi")))
                    
                    # Calculate squared differences
                    df = df.withColumn("bbi_diff_squared", pow(col("bbi_diff"), 2))
                    
                    # Remove the temporary column
                    df = df.drop("next_bbi")
                else:
                    log_info(f"Warning: 'bbi' column not found in {file_path}, skipping BBI calculations")
                
                # Reorder columns to put participant_id first
                columns = df.columns
                first_cols = ["participant_id"]
                
                other_cols = [c for c in columns if c not in first_cols]
                df = df.select(*(first_cols + other_cols))
                
                log_info(f"Processed columns: {df.columns}")
                
                # Record total records
                record_count = df.count()
                log_info(f"Final record count: {record_count}")
                
                # Determine destination path
                output_path = f"s3://{destination_bucket_name}/{file_path}"
                
                # Ensure output directory exists
                output_dir = os.path.dirname(file_path)
                if output_dir:
                    try:
                        s3_client.put_object(Bucket=destination_bucket_name, Key=f"{output_dir}/", Body='')
                    except Exception as e:
                        log_info(f"Note: Could not create directory structure: {str(e)}")
                
                # Write CSV directly with Spark
                log_info(f"Writing output to: {output_path}")
                
                # Extract the directory path and filename
                output_dir_path = os.path.dirname(file_path)
                output_file_name = os.path.basename(file_path)
                
                # Create a temporary directory name (without .csv extension)
                temp_output_dir = output_dir_path + "/temp_" + output_file_name.replace(".csv", "")
                temp_output_path = f"s3://{destination_bucket_name}/{temp_output_dir}"
                
                try:
                    # Write to a temporary directory first
                    df.coalesce(1).write \
                      .mode("overwrite") \
                      .option("header", "true") \
                      .csv(temp_output_path)
                    
                    # Now find the part file in the temporary directory
                    part_files = []
                    for page in paginator.paginate(Bucket=destination_bucket_name, Prefix=temp_output_dir):
                        if "Contents" in page:
                            for obj in page["Contents"]:
                                if obj["Key"].endswith(".csv") and "part-" in obj["Key"]:
                                    part_files.append(obj["Key"])
                    
                    if part_files:
                        # Get the first part file
                        source_part_file = part_files[0]
                        
                        # Copy it to the final destination
                        final_path = file_path
                        
                        log_info(f"Copying from {source_part_file} to {final_path}")
                        
                        # Use S3 copy operation
                        s3_client.copy_object(
                            Bucket=destination_bucket_name,
                            CopySource={"Bucket": destination_bucket_name, "Key": source_part_file},
                            Key=final_path
                        )
                        
                        log_info(f"Successfully copied file to final destination: {final_path}")
                        
                        # Clean up temporary directory
                        cleanup_objects = []
                        for page in paginator.paginate(Bucket=destination_bucket_name, Prefix=temp_output_dir):
                            if "Contents" in page:
                                cleanup_objects.extend([{"Key": obj["Key"]} for obj in page["Contents"]])
                        
                        if cleanup_objects:
                            s3_client.delete_objects(
                                Bucket=destination_bucket_name,
                                Delete={"Objects": cleanup_objects}
                            )
                            log_info(f"Cleaned up {len(cleanup_objects)} temporary files")
                        
                        # Clean up the intermediate S3 file
                        try:
                            s3_client.delete_object(Bucket=source_bucket_name, Key=temp_s3_key)
                            log_info(f"Cleaned up intermediate S3 file")
                        except Exception as e:
                            log_info(f"Note: Could not delete intermediate S3 file: {str(e)}")
                        
                        log_info(f"Successfully processed: {file_path}")
                        return True, None, record_count
                    else:
                        log_error(f"No part files found in temporary directory: {temp_output_dir}")
                        return False, f"No part files found in temporary directory", 0
                except Exception as e:
                    log_error(f"Error writing final CSV: {str(e)}", traceback.format_exc())
                    return False, f"Error writing final CSV: {str(e)}", 0
            except Exception as e:
                log_error(f"Error reading with Spark: {str(e)}")
                return False, f"Error reading with Spark: {str(e)}", 0
        except Exception as e:
            error_msg = f"Error processing file {file_path}: {str(e)}"
            log_error(error_msg)
            return False, error_msg, 0
    
    # Process each file
    for file_path in bbi_files:
        success, error, record_count = process_bbi_file(file_path)
        if success:
            processed_files.append(file_path)
            total_records_processed += record_count
        else:
            failed_files.append({"file": file_path, "error": error})
    
    # Clean up all temporary S3 files
    try:
        log_info(f"Cleaning up temporary S3 folder: s3://{source_bucket_name}/{temp_s3_prefix}")
        # List all objects in the temp folder
        temp_objects = []
        for page in paginator.paginate(Bucket=source_bucket_name, Prefix=temp_s3_prefix):
            if "Contents" in page:
                temp_objects.extend([{"Key": obj["Key"]} for obj in page["Contents"]])
        
        # Delete all objects in the temp folder if any exist
        if temp_objects:
            s3_client.delete_objects(
                Bucket=source_bucket_name,
                Delete={"Objects": temp_objects}
            )
            log_info(f"Deleted {len(temp_objects)} temporary objects")
        else:
            log_info("No temporary objects found to clean up")
            
    except Exception as e:
        log_info(f"Note: Error cleaning up temporary S3 files: {str(e)}")
    
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