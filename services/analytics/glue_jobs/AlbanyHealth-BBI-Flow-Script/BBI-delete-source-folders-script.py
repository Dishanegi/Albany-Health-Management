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

# Get bucket name from default arguments passed by CDK, with fallback default
source_bucket_name = get_optional_arg('SOURCE_BUCKET', 'albanyhealthsource-s3bucket-dev')

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
    log_info(f"Starting deletion of ALL folders and objects from source bucket: {source_bucket_name}")
    
    # Create S3 client
    s3_client = boto3.client('s3')
    
    # List all objects in the bucket using pagination to handle large buckets
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # Track deletion statistics
    total_objects = 0
    deleted_objects = 0
    
    log_info("Listing all objects in source bucket...")
    
    # Process objects in batches
    for page in paginator.paginate(Bucket=source_bucket_name):
        if 'Contents' in page:
            object_batch = []
            for obj in page['Contents']:
                object_batch.append({'Key': obj['Key']})
                total_objects += 1
                
                # S3 delete_objects can handle up to 1000 keys at once
                if len(object_batch) >= 1000:
                    try:
                        response = s3_client.delete_objects(
                            Bucket=source_bucket_name,
                            Delete={'Objects': object_batch}
                        )
                        
                        # Count successfully deleted objects
                        deleted_in_batch = len(object_batch)
                        if 'Errors' in response and response['Errors']:
                            # Subtract any errors from the count
                            deleted_in_batch -= len(response['Errors'])
                            for error in response['Errors']:
                                log_error(f"Failed to delete {error['Key']}: {error['Message']}")
                        
                        deleted_objects += deleted_in_batch
                        log_info(f"Deleted batch of {deleted_in_batch} objects, progress: {deleted_objects}/{total_objects}")
                    except Exception as batch_error:
                        log_error(f"Error deleting batch", batch_error)
                    object_batch = []
            
            # Delete any remaining objects in the batch
            if object_batch:
                try:
                    response = s3_client.delete_objects(
                        Bucket=source_bucket_name,
                        Delete={'Objects': object_batch}
                    )
                    
                    deleted_in_batch = len(object_batch)
                    if 'Errors' in response and response['Errors']:
                        deleted_in_batch -= len(response['Errors'])
                        for error in response['Errors']:
                            log_error(f"Failed to delete {error['Key']}: {error['Message']}")
                    
                    deleted_objects += deleted_in_batch
                    log_info(f"Deleted batch of {deleted_in_batch} objects, progress: {deleted_objects}/{total_objects}")
                except Exception as batch_error:
                    log_error(f"Error deleting final batch", batch_error)
    
    # Check for versioned objects if the bucket has versioning enabled
    try:
        versioning = s3_client.get_bucket_versioning(Bucket=source_bucket_name)
        if 'Status' in versioning and versioning['Status'] == 'Enabled':
            log_info("Bucket has versioning enabled. Deleting versioned objects...")
            
            version_paginator = s3_client.get_paginator('list_object_versions')
            versions_deleted = 0
            
            for version_page in version_paginator.paginate(Bucket=source_bucket_name):
                version_batch = []
                
                # Handle versions
                if 'Versions' in version_page:
                    for version in version_page['Versions']:
                        version_batch.append({
                            'Key': version['Key'],
                            'VersionId': version['VersionId']
                        })
                        
                        if len(version_batch) >= 1000:
                            try:
                                s3_client.delete_objects(
                                    Bucket=source_bucket_name,
                                    Delete={'Objects': version_batch}
                                )
                                versions_deleted += len(version_batch)
                                log_info(f"Deleted {len(version_batch)} versioned objects, total: {versions_deleted}")
                            except Exception as version_error:
                                log_error(f"Error deleting versioned objects batch", version_error)
                            version_batch = []
                
                # Handle delete markers
                if 'DeleteMarkers' in version_page:
                    for marker in version_page['DeleteMarkers']:
                        version_batch.append({
                            'Key': marker['Key'],
                            'VersionId': marker['VersionId']
                        })
                        
                        if len(version_batch) >= 1000:
                            try:
                                s3_client.delete_objects(
                                    Bucket=source_bucket_name,
                                    Delete={'Objects': version_batch}
                                )
                                versions_deleted += len(version_batch)
                                log_info(f"Deleted {len(version_batch)} delete markers, total: {versions_deleted}")
                            except Exception as marker_error:
                                log_error(f"Error deleting delete markers batch", marker_error)
                            version_batch = []
                
                # Delete any remaining objects in the batch
                if version_batch:
                    try:
                        s3_client.delete_objects(
                            Bucket=source_bucket_name,
                            Delete={'Objects': version_batch}
                        )
                        versions_deleted += len(version_batch)
                        log_info(f"Deleted {len(version_batch)} versioned objects/markers, total: {versions_deleted}")
                    except Exception as version_error:
                        log_error(f"Error deleting final versioned batch", version_error)
            
            log_info(f"Successfully deleted {versions_deleted} versioned objects and delete markers")
    except Exception as version_error:
        log_error(f"Error processing versioned objects", version_error)
    
    # Summary
    log_info("=" * 60)
    log_info("Deletion Summary:")
    log_info(f"  Total objects found: {total_objects}")
    log_info(f"  Objects deleted: {deleted_objects}")
    log_info(f"  Source bucket: {source_bucket_name}")
    log_info("=" * 60)
    
    if deleted_objects == total_objects:
        log_info("Successfully deleted all folders and objects from source bucket")
    else:
        log_error(f"Warning: Only {deleted_objects}/{total_objects} objects were successfully deleted")
    
except Exception as e:
    log_error(f"Fatal error while deleting source bucket folders", e)
    raise e

job.commit()
log_info("Job completed successfully")

