import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime

# Initialize the script
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# IMPORTANT: Assign your bucket name here
bucket_name = "albanyhealthprocesseds3bucket-dev" 

# Log function for tracking operations
def log_info(message):
    """Log information to console"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"[{timestamp}] INFO: {message}")

def log_error(message, error=None):
    """Log errors to console"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"[{timestamp}] ERROR: {message}")
    if error:
        print(f"Error details: {str(error)}")

try:
    log_info(f"Starting deletion of all objects in bucket: {bucket_name}")
    
    # Create S3 client
    s3_client = boto3.client('s3')
    
    # List all objects in the bucket using pagination to handle large buckets
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # Track deletion statistics
    total_objects = 0
    deleted_objects = 0
    
    # Process objects in batches
    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' in page:
            object_batch = []
            for obj in page['Contents']:
                object_batch.append({'Key': obj['Key']})
                total_objects += 1
                
                # S3 delete_objects can handle up to 1000 keys at once
                if len(object_batch) >= 1000:
                    response = s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': object_batch}
                    )
                    deleted_objects += len(object_batch)
                    log_info(f"Deleted batch of {len(object_batch)} objects, progress: {deleted_objects}/{total_objects}")
                    object_batch = []
            
            # Delete any remaining objects in the batch
            if object_batch:
                response = s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': object_batch}
                )
                deleted_objects += len(object_batch)
                log_info(f"Deleted batch of {len(object_batch)} objects, progress: {deleted_objects}/{total_objects}")
    
    # Check for versioned objects if the bucket has versioning enabled
    try:
        versioning = s3_client.get_bucket_versioning(Bucket=bucket_name)
        if 'Status' in versioning and versioning['Status'] == 'Enabled':
            log_info("Bucket has versioning enabled. Deleting versioned objects...")
            
            version_paginator = s3_client.get_paginator('list_object_versions')
            versions_deleted = 0
            
            for version_page in version_paginator.paginate(Bucket=bucket_name):
                version_batch = []
                
                # Handle versions
                if 'Versions' in version_page:
                    for version in version_page['Versions']:
                        version_batch.append({
                            'Key': version['Key'],
                            'VersionId': version['VersionId']
                        })
                        
                        if len(version_batch) >= 1000:
                            s3_client.delete_objects(
                                Bucket=bucket_name,
                                Delete={'Objects': version_batch}
                            )
                            versions_deleted += len(version_batch)
                            log_info(f"Deleted {len(version_batch)} versioned objects, total: {versions_deleted}")
                            version_batch = []
                
                # Handle delete markers
                if 'DeleteMarkers' in version_page:
                    for marker in version_page['DeleteMarkers']:
                        version_batch.append({
                            'Key': marker['Key'],
                            'VersionId': marker['VersionId']
                        })
                        
                        if len(version_batch) >= 1000:
                            s3_client.delete_objects(
                                Bucket=bucket_name,
                                Delete={'Objects': version_batch}
                            )
                            versions_deleted += len(version_batch)
                            log_info(f"Deleted {len(version_batch)} delete markers, total: {versions_deleted}")
                            version_batch = []
                
                # Delete any remaining objects in the batch
                if version_batch:
                    s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': version_batch}
                    )
                    versions_deleted += len(version_batch)
                    log_info(f"Deleted {len(version_batch)} versioned objects/markers, total: {versions_deleted}")
            
            log_info(f"Successfully deleted {versions_deleted} versioned objects and delete markers")
    except Exception as version_error:
        log_error(f"Error processing versioned objects", version_error)
    
    log_info(f"Successfully deleted {deleted_objects} objects from bucket {bucket_name}")
    
except Exception as e:
    log_error(f"Error while deleting bucket contents", e)
    raise e

job.commit()