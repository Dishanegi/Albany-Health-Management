import os
import boto3
import logging
from typing import Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Triggered by EventBridge rule once survey data has been merged.
    Deletes all files from the source bucket.
    """
    source_bucket = os.environ.get('SOURCE_BUCKET')

    if not source_bucket:
        raise ValueError("SOURCE_BUCKET environment variable is not set")

    logger.info(f"Starting deletion of all files in bucket '{source_bucket}'")

    deleted_files = []
    failed_files = []

    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=source_bucket)

        objects_to_delete = []
        for page in pages:
            for obj in page.get('Contents', []):
                objects_to_delete.append({'Key': obj['Key']})

        if not objects_to_delete:
            logger.info("No files found in bucket - nothing to delete")
            return {
                'statusCode': 200,
                'body': {
                    'message': 'No files found to delete',
                    'sourceBucket': source_bucket,
                    'deletedCount': 0,
                }
            }

        logger.info(f"Found {len(objects_to_delete)} files to delete")

        # S3 delete_objects supports up to 1000 keys per request
        chunk_size = 1000
        for i in range(0, len(objects_to_delete), chunk_size):
            chunk = objects_to_delete[i:i + chunk_size]

            response = s3.delete_objects(
                Bucket=source_bucket,
                Delete={
                    'Objects': chunk,
                    'Quiet': False
                }
            )

            for deleted in response.get('Deleted', []):
                deleted_files.append(deleted['Key'])
                logger.info(f"Deleted: {deleted['Key']}")

            for error in response.get('Errors', []):
                failed_files.append({
                    'key': error['Key'],
                    'code': error['Code'],
                    'message': error['Message']
                })
                logger.error(f"Failed to delete {error['Key']}: {error['Code']} - {error['Message']}")

        logger.info(f"Deletion complete. Deleted: {len(deleted_files)}, Failed: {len(failed_files)}")

        return {
            'statusCode': 200,
            'body': {
                'sourceBucket': source_bucket,
                'deletedCount': len(deleted_files),
                'failedCount': len(failed_files),
                'deletedFiles': deleted_files,
                'failedFiles': failed_files,
            }
        }

    except Exception as e:
        logger.error(f"Unexpected error during deletion: {str(e)}")
        raise