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

def delete_folder(s3_client, bucket, folder_path):
    """Delete all objects in a folder and the folder itself"""
    try:
        # List all objects in the folder
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=folder_path
        )
        
        if "Contents" in response:
            # Delete all objects in the folder
            for obj in response["Contents"]:
                s3_client.delete_object(
                    Bucket=bucket,
                    Key=obj["Key"]
                )
            log_info(f"Successfully deleted folder: {folder_path}")
        else:
            log_info(f"No objects found in folder: {folder_path}")
    except Exception as e:
        log_error(f"Error deleting folder {folder_path}", str(e))

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

    # S3 bucket and output path - using environment variables
    # Get bucket names from default arguments passed by CDK, with fallback defaults
    source_bucket_name = get_optional_arg("SOURCE_BUCKET", "albanyhealthprocessed-s3bucket-dev")
    target_bucket_name = get_optional_arg("TARGET_BUCKET", "albanyhealthmerged-s3bucket-dev")
    output_path = f"s3://{source_bucket_name}/initial_append"

    log_info("Job initialized successfully")

    # This section lists ALL files in the bucket to help understand the structure
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")

    # Log all files and folders to understand structure
    log_info("Listing all objects in bucket to understand structure...")
    all_paths = []

    for page in paginator.paginate(Bucket=source_bucket_name):
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                all_paths.append(key)

    # Log a sample of paths to understand the structure
    sample_paths = all_paths[:50] if len(all_paths) > 50 else all_paths
    log_info(f"Sample of paths in bucket: {sample_paths}")

    # Look for our data types anywhere in the paths
    heart_rate_files = []
    sleep_stage_files = []
    step_files = []
    respiration_files = []
    stress_files = []
    pulse_ox_files = []

    data_type_keywords = {
        "garmin-device-heart-rate": heart_rate_files,
        "garmin-connect-sleep-stage": sleep_stage_files,
        "garmin-device-step": step_files,
        "garmin-device-respiration": respiration_files,
        "garmin-device-stress": stress_files,
        "garmin-device-pulse-ox": pulse_ox_files,
    }

    # Alternative keywords in case the exact folder names don't match
    alternative_keywords = {
        "heart": heart_rate_files,
        "sleep": sleep_stage_files,
        "step": step_files,
        "respiration": respiration_files,
        "stress": stress_files,
        "pulse": pulse_ox_files,
        "spo2": pulse_ox_files,
        "oxygen": pulse_ox_files,
    }

    for path in all_paths:
        if path.endswith(".csv"):
            # Check primary keywords
            for keyword, file_list in data_type_keywords.items():
                if keyword in path:
                    file_list.append(path)
                    break
            else:
                # If no primary keyword matched, check alternative keywords
                for keyword, file_list in alternative_keywords.items():
                    if keyword in path.lower():
                        file_list.append(path)
                        break

    # Log what we found
    log_info(f"Found {len(heart_rate_files)} heart rate files")
    log_info(f"Found {len(sleep_stage_files)} sleep stage files")
    log_info(f"Found {len(step_files)} step files")
    log_info(f"Found {len(respiration_files)} respiration files")
    log_info(f"Found {len(stress_files)} stress files")
    log_info(f"Found {len(pulse_ox_files)} pulse-ox files")

    # Process each file type
    def process_files_by_type(file_list, data_type_name):
        if not file_list:
            return None

        log_info(f"Processing {len(file_list)} files for {data_type_name}")
        
        # Create a list of all file paths
        file_paths = [f"s3://{source_bucket_name}/{file}" for file in file_list]
        
        try:
            # Read all files at once and combine them
            combined_df = None
            for file_path in file_paths:
                log_info(f"Reading file: {file_path}")
                current_df = glueContext.create_dynamic_frame.from_options(
                    format_options={
                        "quoteChar": '"',
                        "withHeader": True,
                        "separator": ",",
                    },
                    connection_type="s3",
                    format="csv",
                    connection_options={"paths": [file_path]},
                    transformation_ctx=f"{data_type_name}_df_{file_path.split('/')[-1]}",
                ).toDF()

                if combined_df is None:
                    combined_df = current_df
                else:
                    combined_df = combined_df.union(current_df)

            if combined_df is not None:
                # Display schema for debugging
                log_info(f"{data_type_name} schema: {combined_df.schema.simpleString()}")
                log_info(f"{data_type_name} columns: {combined_df.columns}")

                # Extract participantId from composite key if present
                if "participantid_timestamp" in combined_df.columns:
                    combined_df = combined_df.withColumn(
                        "participant_id",
                        split(col("participantid_timestamp"), "_").getItem(0),
                    )
                    combined_df = combined_df.withColumn(
                        "timestamp", split(col("participantid_timestamp"), "_").getItem(1)
                    )

                log_info(f"Successfully combined {combined_df.count()} total {data_type_name} records")
                return combined_df
            else:
                log_error(f"Failed to load any {data_type_name} data")
                return None

        except Exception as e:
            log_error(f"Error processing {data_type_name} files", str(e))
            return None

    # Process each data type and save to initial_append folder
    data_types = {
        "garmin-device-heart-rate": heart_rate_files,
        "garmin-connect-sleep-stage": sleep_stage_files,
        "garmin-device-step": step_files,
        "garmin-device-respiration": respiration_files,
        "garmin-device-stress": stress_files,
        "garmin-device-pulse-ox": pulse_ox_files,
    }

    # Keep track of parent folders to delete
    parent_folders = set()

    # Group files by patient identifier
    for data_type, files in data_types.items():
        log_info(f"Processing {data_type} files...")
        
        # Group files by patient identifier
        patient_files = {}
        for file in files:
            # Extract patient identifier from path (second folder name)
            path_parts = file.split('/')
            if len(path_parts) >= 2:
                patient_id = path_parts[1]  # e.g., "Testing-1_cd037752"
                if patient_id not in patient_files:
                    patient_files[patient_id] = []
                patient_files[patient_id].append(file)
                # Add parent folder for later deletion
                parent_folders.add(path_parts[0])

        # Process files for each patient
        for patient_id, patient_file_list in patient_files.items():
            log_info(f"Processing {data_type} files for patient {patient_id}")
            df = process_files_by_type(patient_file_list, f"{data_type} for {patient_id}")
            
            if df is not None:
                # Save the processed data to initial_append/patient_id folder
                output_file_path = f"{output_path}/{patient_id}/{data_type}.csv"
                log_info(f"Saving processed {data_type} data to {output_file_path}")
                
                try:
                    # Write as a single CSV file using DataFrame operations
                    (df.coalesce(1)  # Ensure single partition
                       .write
                       .mode("overwrite")  # Overwrite if exists
                       .options(header='true', delimiter=',', quote='"')  # CSV options
                       .csv(output_file_path.replace('.csv', '_temp')))  # Temporary location
                    
                    # Find the generated part file
                    response = s3_client.list_objects_v2(
                        Bucket=source_bucket_name,
                        Prefix=f"initial_append/{patient_id}/{data_type}_temp"
                    )
                    
                    if "Contents" in response:
                        # Get the part file (should be only one due to coalesce)
                        part_file = next((obj["Key"] for obj in response["Contents"] 
                                        if obj["Key"].endswith(".csv") or "part-" in obj["Key"]), None)
                        
                        if part_file:
                            # Copy to final destination with proper name
                            s3_client.copy_object(
                                Bucket=source_bucket_name,
                                CopySource={'Bucket': source_bucket_name, 'Key': part_file},
                                Key=f"initial_append/{patient_id}/{data_type}.csv"
                            )
                            
                            # Delete temporary directory and its contents
                            temp_objects = s3_client.list_objects_v2(
                                Bucket=source_bucket_name,
                                Prefix=f"initial_append/{patient_id}/{data_type}_temp"
                            )
                            
                            if "Contents" in temp_objects:
                                for obj in temp_objects["Contents"]:
                                    s3_client.delete_object(
                                        Bucket=source_bucket_name,
                                        Key=obj["Key"]
                                    )
                            
                            log_info(f"Successfully created single CSV file: initial_append/{patient_id}/{data_type}.csv")
                        else:
                            log_error(f"No CSV file found in temporary directory for {patient_id}/{data_type}")
                    
                except Exception as e:
                    log_error(f"Error creating CSV file for {patient_id}/{data_type}", str(e))
                    raise e
                
                log_info(f"Successfully saved {data_type} data for {patient_id} with {df.count()} records")

    # Delete all parent folders after successful processing
    try:
        log_info("Deleting parent folders...")
        for parent_folder in parent_folders:
            # List all objects under the parent folder
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=source_bucket_name,
                Prefix=f"{parent_folder}/"
            )
            
            # Delete all objects under the parent folder
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        s3_client.delete_object(
                            Bucket=source_bucket_name,
                            Key=obj["Key"]
                        )
            
            # Delete the parent folder marker if it exists
            s3_client.delete_object(
                Bucket=source_bucket_name,
                Key=f"{parent_folder}/"
            )
            
            log_info(f"Deleted parent folder and all contents: {parent_folder}")
            
    except Exception as e:
        log_error("Error deleting parent folders", str(e))
        raise e

    job.commit()
    log_info("Job completed successfully")

except Exception as e:
    log_error("Fatal error in main job flow", str(e))
    raise e 