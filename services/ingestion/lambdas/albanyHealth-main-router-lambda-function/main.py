import json
import os
import boto3
from botocore.exceptions import ClientError

def extract_folder_name(object_key):
    """
    Extracts the folder name from object key
    Example input: 'Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/250204_garmin-device-heart-rate_Testing-1_cd037752.csv'
    Should return: 'garmin-device-heart-rate'
    """
    path_segments = object_key.split('/')
    if len(path_segments) >= 3:  # Ensure we have enough segments
        return path_segments[2]  # Return the third segment (index 2)
    return ''

def get_queue_url_for_folder(folder_name):
    """
    Returns the appropriate SQS queue URL based on folder name
    Uses environment variables that are set by CDK (environment-agnostic)
    """
    queue_mapping = {
        'garmin-device-heart-rate': os.environ.get('HEALTH_HEART_RATE_QUEUE'),
        'garmin-connect-sleep-stage': os.environ.get('HEALTH_SLEEP_QUEUE'),
        'garmin-device-step': os.environ.get('HEALTH_STEP_QUEUE')
    }
    
    # List of folders that should use the default queue
    default_queue_folders = [
        'garmin-device-pulse-ox',
        'garmin-device-respiration',
        'garmin-device-stress'
    ]
    
    # Get the queue URL based on folder
    queue_url = queue_mapping.get(folder_name)
    
    # Use default queue only for specific folders
    if not queue_url and folder_name in default_queue_folders:
        print(f"Using default queue for folder {folder_name}")
        queue_url = os.environ.get('HEALTH_OTHERS_QUEUE')
    
    return queue_url

def lambda_handler(event, context):
    # Initialize AWS clients
    sqs = boto3.client('sqs')
    
    processed_messages = []
    failed_messages = []

    # Add this at the start of your lambda_handler function
    print(f"DEBUG: Full event: {json.dumps(event)}")

    print(f"DEBUG: Starting lambda_handler with {len(event.get('Records', []))} records")

    for record in event['Records']:
        try:
            # Extract the body from SQS message
            body_json_string = record['body']
            print(f"DEBUG: SQS message body: {body_json_string[:200]}...")  # Print first 200 chars
            
            body_content = json.loads(body_json_string)

            # Extract S3 details from the nested message
            s3_info = body_content['Records'][0]['s3']
            source_bucket = s3_info['bucket']['name']
            object_key = s3_info['object']['key']

            print(f"DEBUG: Processing message for file: s3://{source_bucket}/{object_key}")
            
            # Extract folder name
            folder_name = extract_folder_name(object_key)
            
            # Get appropriate queue URL
            queue_url = get_queue_url_for_folder(folder_name)
            
            # Skip processing if no matching queue found for this folder
            if not queue_url:
                print(f"DEBUG: No queue URL found for folder {folder_name}, skipping processing")
                continue

            # Prepare message for processing queue
            message_body = json.dumps({
                'source_bucket': source_bucket,
                'object_key': object_key,
                'folder_name': folder_name
            })

            # Send message to appropriate processing queue
            try:
                response = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=message_body
                )
                processed_messages.append({
                    'file': object_key,
                    'queue': queue_url,
                    'messageId': response['MessageId']
                })
                print(f"DEBUG: Message sent to queue {queue_url} for file {object_key}")
            
            except ClientError as e:
                print(f"DEBUG: Error sending message to queue: {str(e)}")
                failed_messages.append({
                    'file': object_key,
                    'queue': queue_url,
                    'error': str(e)
                })

        except Exception as e:
            print(f"DEBUG: Error processing message: {str(e)}")
            # Print full stack trace
            import traceback
            traceback.print_exc()
            
            error_context = {
                'file': object_key if 'object_key' in locals() else 'unknown',
                'error': str(e)
            }
            failed_messages.append(error_context)
            print(f"DEBUG: Error details: {json.dumps(error_context)}")

    # Prepare summary response
    summary = {
        'processed_messages': len(processed_messages),
        'failed_messages': len(failed_messages),
        'failures': failed_messages if failed_messages else None,
        'processed': processed_messages if processed_messages else None
    }

    print(f"DEBUG: Lambda execution complete. Summary: {json.dumps(summary, indent=2)}")
    return {
        'statusCode': 200,
        'body': json.dumps(summary, indent=2)
    }