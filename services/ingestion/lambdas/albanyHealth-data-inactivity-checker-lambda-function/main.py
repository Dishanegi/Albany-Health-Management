import json
import os
import boto3
import logging
from datetime import datetime
from typing import Dict, List, Any
from botocore.exceptions import ClientError
import hashlib
import urllib.parse
import time
import random

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Expected file counts per patient
EXPECTED_FILE_COUNTS = {
    'garmin-device-stress': 7,
    'garmin-device-step': 7,
    'garmin-device-respiration': 7,
    'garmin-device-pulse-ox': 7,
    'garmin-device-heart-rate': 7,
    'garmin-connect-sleep-stage': 1
}

# Completion strategy configuration
COMPLETION_THRESHOLD_PERCENTAGE = 80
TOTAL_EXPECTED_FILES_PER_PATIENT = sum(EXPECTED_FILE_COUNTS.values())

# Retry configuration for race condition handling
MAX_RETRY_ATTEMPTS = 5
BASE_RETRY_DELAY = 0.1  # 100ms base delay
MAX_RETRY_DELAY = 2.0   # 2 second max delay

# EventBridge configuration - loaded from environment variables
def get_eventbridge_config():
    """
    Get EventBridge configuration from environment variables
    """
    return {
        'bus_name': os.environ.get('EVENTBRIDGE_BUS_NAME', 'default'),
        'completion_rule_name': os.environ.get('EVENTBRIDGE_COMPLETION_RULE_NAME', 'trigger-merge-patient-health-metrics'),
        'processing_rule_name': os.environ.get('EVENTBRIDGE_PROCESSING_RULE_NAME', 'trigger-patients-bbi-flow'),
    }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function with race condition protection using S3 conditional writes
    """
    
    # Initialize AWS clients
    s3 = boto3.client('s3')
    eventbridge = boto3.client('events')
    
    processed_messages = []
    failed_messages = []
    duplicate_messages = []
    batch_completions = []
    race_condition_retries = []
    
    # Track processed events within this execution
    processed_events = set()
    
    logger.info(f"Received {len(event['Records'])} messages from SQS")
    
    # Process each SQS message
    for record in event['Records']:
        try:
            message_id = record['messageId']
            message_body = json.loads(record['body'])
            
            logger.info(f"Processing message {message_id}")
            
            # Extract S3 record from SQS message
            s3_record = extract_s3_record_from_sqs_message(message_body)
            
            if s3_record:
                # Create event fingerprint for deduplication
                event_fingerprint = create_event_fingerprint(s3_record)
                
                # Skip if already processed in this execution
                if event_fingerprint in processed_events:
                    logger.info(f"Skipping duplicate event in same execution: {event_fingerprint}")
                    duplicate_messages.append({
                        'messageId': message_id,
                        'reason': 'duplicate_in_execution',
                        'fingerprint': event_fingerprint
                    })
                    continue
                
                processed_events.add(event_fingerprint)
                
                # Process with race condition protection
                result = process_s3_event_with_race_protection(
                    s3_record, s3, eventbridge, event_fingerprint
                )
                
                if result['success']:
                    processed_messages.append({
                        'messageId': message_id,
                        'status': 'processed',
                        'timestamp': datetime.utcnow().isoformat(),
                        'result': result['data'],
                        'fingerprint': event_fingerprint,
                        'retry_attempts': result.get('retry_attempts', 0)
                    })
                    
                    # Track race condition retries for monitoring
                    if result.get('retry_attempts', 0) > 0:
                        race_condition_retries.append({
                            'messageId': message_id,
                            'attempts': result['retry_attempts'],
                            'final_status': 'success'
                        })
                    
                    # Track batch completions
                    if result.get('batch_completed'):
                        batch_completions.append(result['batch_info'])
                        
                elif result.get('duplicate'):
                    duplicate_messages.append({
                        'messageId': message_id,
                        'reason': 'already_processed',
                        'fingerprint': event_fingerprint,
                        'timestamp': datetime.utcnow().isoformat()
                    })
                else:
                    failed_messages.append({
                        'messageId': message_id,
                        'error': result['error'],
                        'timestamp': datetime.utcnow().isoformat(),
                        'retry_attempts': result.get('retry_attempts', 0)
                    })
                    
                    # Track failed race condition retries
                    if result.get('retry_attempts', 0) > 0:
                        race_condition_retries.append({
                            'messageId': message_id,
                            'attempts': result['retry_attempts'],
                            'final_status': 'failed'
                        })
            else:
                # Handle invalid S3 event format
                error_msg = f"Invalid S3 event format in SQS message"
                logger.error(f"Message {message_id}: {error_msg}")
                failed_messages.append({
                    'messageId': message_id,
                    'error': error_msg,
                    'timestamp': datetime.utcnow().isoformat()
                })
                    
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON in message body: {str(e)}"
            logger.error(f"Message {message_id}: {error_msg}")
            failed_messages.append({
                'messageId': message_id,
                'error': error_msg,
                'timestamp': datetime.utcnow().isoformat()
            })
            
        except Exception as e:
            error_msg = f"Unexpected error processing message: {str(e)}"
            logger.error(f"Message {message_id}: {error_msg}")
            failed_messages.append({
                'messageId': message_id,
                'error': error_msg,
                'timestamp': datetime.utcnow().isoformat()
            })
    
    # Log summary with race condition statistics
    logger.info(f"Processing complete. Successful: {len(processed_messages)}, "
                f"Failed: {len(failed_messages)}, Duplicates: {len(duplicate_messages)}, "
                f"Batch Completions: {len(batch_completions)}, "
                f"Race Condition Retries: {len(race_condition_retries)}")
    
    if race_condition_retries:
        total_retry_attempts = sum(r['attempts'] for r in race_condition_retries)
        logger.info(f"Race condition handling: {len(race_condition_retries)} messages required retries, "
                   f"total retry attempts: {total_retry_attempts}")
    
    # Handle failed messages
    if failed_messages:
        handle_failed_messages(failed_messages)
    
    # Return response
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processedCount': len(processed_messages),
            'failedCount': len(failed_messages),
            'duplicateCount': len(duplicate_messages),
            'batchCompletionsCount': len(batch_completions),
            'raceConditionRetries': len(race_condition_retries),
            'processedMessages': processed_messages,
            'failedMessages': failed_messages,
            'duplicateMessages': duplicate_messages,
            'batchCompletions': batch_completions,
            'raceConditionStats': race_condition_retries,
            'timestamp': datetime.utcnow().isoformat()
        })
    }

def extract_s3_record_from_sqs_message(message_body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract S3 event record from SQS message body (S3 → SQS → Lambda flow only)
    """
    try:
        # Case 1: Standard S3 event notification format
        if 'Records' in message_body:
            logger.info("Processing S3 event notification format")
            
            # Find the first S3 record in the Records array
            for record in message_body['Records']:
                if record.get('eventSource') == 'aws:s3':
                    logger.info(f"Found S3 record: {record.get('eventName', 'unknown')} on {record.get('s3', {}).get('bucket', {}).get('name')}/{record.get('s3', {}).get('object', {}).get('key')}")
                    return record
            
            logger.warning("No S3 records found in Records array")
            return None
            
        # Case 2: Already a single S3 record (rare but possible)
        elif message_body.get('eventSource') == 'aws:s3':
            logger.info("Processing single S3 event record")
            return message_body
            
        else:
            logger.error(f"Invalid message format - expected S3 event notification. Keys found: {list(message_body.keys())}")
            return None
            
    except Exception as e:
        logger.error(f"Error extracting S3 record from SQS message: {str(e)}")
        return None

def create_event_fingerprint(s3_record: Dict[str, Any]) -> str:
    """
    Create a unique fingerprint for S3 events to detect duplicates
    """
    try:
        s3_info = s3_record.get('s3', {})
        bucket = s3_info.get('bucket', {}).get('name')
        key = s3_info.get('object', {}).get('key')
        etag = s3_info.get('object', {}).get('eTag', '')
        event_name = s3_record.get('eventName', '')
        
        # URL decode the key
        if key:
            key = urllib.parse.unquote_plus(key)
        
        # Create fingerprint from bucket, key, etag, and event type
        fingerprint_data = f"{bucket}#{key}#{etag}#{event_name}"
        fingerprint = hashlib.md5(fingerprint_data.encode()).hexdigest()
        
        logger.debug(f"Created fingerprint {fingerprint} for {fingerprint_data}")
        return fingerprint
        
    except Exception as e:
        logger.error(f"Error creating event fingerprint: {str(e)}")
        return hashlib.md5(str(s3_record).encode()).hexdigest()

def process_s3_event_with_race_protection(s3_record: Dict[str, Any], s3_client, eventbridge_client, event_fingerprint: str) -> Dict[str, Any]:
    """
    Process S3 event with race condition protection using exponential backoff retries
    """
    try:
        # Extract bucket and key
        s3_info = s3_record.get('s3', {})
        bucket_name = s3_info.get('bucket', {}).get('name')
        object_key = s3_info.get('object', {}).get('key')
        
        if object_key:
            object_key = urllib.parse.unquote_plus(object_key)
        
        logger.info(f"Processing S3 event - Bucket: {bucket_name}, Key: {object_key}")
        
        if not bucket_name or not object_key:
            raise ValueError(f"Missing bucket name ({bucket_name}) or object key ({object_key})")
        
        # Parse the object key to extract batch info
        batch_info = parse_object_key(object_key)
        if not batch_info:
            logger.warning(f"Could not parse batch info from key: {object_key}")
            return process_regular_s3_event(bucket_name, object_key, s3_client)
        
        batch_info['event_fingerprint'] = event_fingerprint
        logger.info(f"Parsed batch info: {batch_info}")
        
        # Update batch.json with race condition protection
        update_result = update_batch_json_with_race_protection(s3_client, bucket_name, batch_info)
        
        # If already processed, return duplicate flag
        if update_result.get('already_processed'):
            logger.info(f"File already processed: {object_key}")
            return {
                'success': True,
                'duplicate': True,
                'data': update_result,
                'retry_attempts': update_result.get('retry_attempts', 0)
            }
        
        # Check if batch just became complete and trigger EventBridge
        batch_completed = False
        if update_result.get('batch_complete') and update_result.get('batch_newly_completed'):
            logger.info(f"Batch {batch_info['batch_folder']} just became complete! Triggering EventBridge rules...")
            
            eventbridge_results = trigger_batch_complete_events(
                eventbridge_client, 
                bucket_name, 
                batch_info, 
                update_result
            )
            
            batch_completed = True
            update_result['eventbridge_results'] = eventbridge_results
        
        # Get object metadata for S3 native events
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            object_size = response.get('ContentLength')
            last_modified = response.get('LastModified').isoformat() if response.get('LastModified') else None
        except ClientError as e:
            logger.warning(f"Could not get object metadata for {object_key}: {e}")
            # Fallback to event data
            object_size = s3_record.get('s3', {}).get('object', {}).get('size', 0)
            last_modified = s3_record.get('eventTime')
        
        return {
            'success': True,
            'duplicate': False,
            'batch_completed': batch_completed,
            'batch_info': batch_info,
            'retry_attempts': update_result.get('retry_attempts', 0),
            'data': {
                'bucket': bucket_name,
                'key': object_key,
                'size': object_size,
                'last_modified': last_modified,
                'batch_info': batch_info,
                'batch_update': update_result,
                'event_source': 's3_native',
                'race_protection_used': update_result.get('retry_attempts', 0) > 0
            }
        }
        
    except ClientError as e:
        error_msg = f"S3 error: {e.response['Error']['Message']}"
        logger.error(error_msg)
        return {
            'success': False,
            'error': error_msg
        }
    except Exception as e:
        error_msg = f"S3 processing error: {str(e)}"
        logger.error(error_msg)
        return {
            'success': False,
            'error': error_msg
        }

def update_batch_json_with_race_protection(s3_client, bucket_name: str, batch_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update batch.json with race condition protection using S3 conditional writes and exponential backoff
    """
    batch_folder = batch_info['batch_folder']
    patient_id = batch_info['patient_id']
    file_type = batch_info['file_type']
    filename = batch_info['filename']
    event_fingerprint = batch_info['event_fingerprint']
    batch_json_key = f"{batch_folder}/batch.json"
    
    logger.info(f"Updating batch.json with race protection: {batch_json_key}")
    
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            # Step 1: Get current batch.json with ETag for conditional updates
            current_etag = None
            batch_data = None
            was_already_complete = False
            
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=batch_json_key)
                batch_data = json.loads(response['Body'].read().decode('utf-8'))
                current_etag = response['ETag'].strip('"')  # Remove quotes from ETag
                was_already_complete = batch_data.get('status') == 'complete'
                logger.info(f"Attempt {attempt + 1}: Found existing batch.json with ETag: {current_etag}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    # Create new batch.json structure
                    batch_data = {
                        'batch_id': batch_folder,
                        'created_at': datetime.utcnow().isoformat(),
                        'patients': {},
                        'processed_events': [],
                        'status': 'processing',
                        'last_updated': datetime.utcnow().isoformat(),
                        'expected_patients': None,
                        'completion_criteria': 'all_patients_complete',
                        'version': 1  # Add version tracking
                    }
                    current_etag = None  # No ETag for new file
                    was_already_complete = False
                    logger.info(f"Attempt {attempt + 1}: Creating new batch.json structure")
                else:
                    raise e
            
            # Step 2: Check if this exact event was already processed (idempotency)
            processed_events = batch_data.get('processed_events', [])
            if event_fingerprint in processed_events:
                logger.info(f"Event {event_fingerprint} already processed - returning early")
                return {
                    'already_processed': True,
                    'event_fingerprint': event_fingerprint,
                    'batch_json_key': batch_json_key,
                    'retry_attempts': attempt
                }
            
            # Step 3: Apply the update logic
            file_id = f"{file_type}/{filename}"
            
            # Initialize patient if not exists
            if patient_id not in batch_data['patients']:
                batch_data['patients'][patient_id] = {
                    'file_counts': {ft: 0 for ft in EXPECTED_FILE_COUNTS.keys()},
                    'processed_files': [],
                    'total_files': 0,
                    'expected_files': sum(EXPECTED_FILE_COUNTS.values()),
                    'is_complete': False,
                    'first_file_received': datetime.utcnow().isoformat(),
                    'last_file_received': datetime.utcnow().isoformat()
                }
                logger.info(f"Initialized new patient: {patient_id}")
            
            patient_data = batch_data['patients'][patient_id]
            
            # Check if this specific file was already processed
            if file_id in patient_data.get('processed_files', []):
                logger.info(f"File {file_id} already processed for patient {patient_id}")
                return {
                    'already_processed': True,
                    'file_already_counted': True,
                    'file_id': file_id,
                    'batch_json_key': batch_json_key,
                    'retry_attempts': attempt
                }
            
            # Apply the update
            processed_events.append(event_fingerprint)
            patient_data['processed_files'].append(file_id)
            patient_data['last_file_received'] = datetime.utcnow().isoformat()
            
            # Update file count
            if file_type in patient_data['file_counts']:
                old_count = patient_data['file_counts'][file_type]
                patient_data['file_counts'][file_type] += 1
                patient_data['total_files'] += 1
                logger.info(f"Updated file count for {file_type}: {old_count} -> {patient_data['file_counts'][file_type]}")
            
            # Calculate completion status
            patient_total_files = sum(patient_data['file_counts'].values())
            patient_expected_files = patient_data['expected_files']
            patient_completion_percentage = (patient_total_files / patient_expected_files) * 100
            
            patient_meets_threshold = patient_completion_percentage >= COMPLETION_THRESHOLD_PERCENTAGE
            patient_traditional_complete = all(
                patient_data['file_counts'][ft] >= expected_count 
                for ft, expected_count in EXPECTED_FILE_COUNTS.items()
            )
            
            patient_complete = patient_meets_threshold or patient_traditional_complete
            patient_data['is_complete'] = patient_complete
            patient_data['completion_percentage'] = round(patient_completion_percentage, 2)
            patient_data['completion_method'] = (
                'traditional' if patient_traditional_complete 
                else 'threshold' if patient_meets_threshold 
                else 'incomplete'
            )
            
            # Calculate batch completion
            total_patients = len(batch_data['patients'])
            complete_patients = sum(1 for p in batch_data['patients'].values() if p['is_complete'])
            all_patients_meet_threshold = all(
                patient['is_complete'] for patient in batch_data['patients'].values()
            )
            
            total_files_in_batch = sum(
                sum(p['file_counts'].values()) for p in batch_data['patients'].values()
            )
            total_expected_files_in_batch = total_patients * TOTAL_EXPECTED_FILES_PER_PATIENT
            batch_completion_percentage = (total_files_in_batch / total_expected_files_in_batch) * 100 if total_expected_files_in_batch > 0 else 0
            
            batch_newly_completed = False
            if all_patients_meet_threshold and batch_data['patients'] and not was_already_complete:
                batch_data['status'] = 'complete'
                batch_data['completed_at'] = datetime.utcnow().isoformat()
                batch_data['completion_strategy'] = 'threshold_based'
                batch_data['completion_threshold'] = COMPLETION_THRESHOLD_PERCENTAGE
                batch_newly_completed = True
                logger.info(f"BATCH {batch_folder} has just become COMPLETE!")
            
            # Update metadata
            batch_data['processed_events'] = processed_events
            batch_data['last_updated'] = datetime.utcnow().isoformat()
            batch_data['version'] = batch_data.get('version', 1) + 1  # Increment version
            batch_data['stats'] = {
                'total_patients': total_patients,
                'complete_patients': complete_patients,
                'incomplete_patients': total_patients - complete_patients,
                'total_files_processed': total_files_in_batch,
                'total_expected_files': total_expected_files_in_batch,
                'batch_completion_percentage': round(batch_completion_percentage, 2),
                'completion_threshold': COMPLETION_THRESHOLD_PERCENTAGE,
                'completion_strategy': 'threshold_based'
            }
            
            # Clean up old processed events
            if len(processed_events) > 1000:
                batch_data['processed_events'] = processed_events[-1000:]
            
            # Step 4: Conditional write to S3 using ETag
            put_object_args = {
                'Bucket': bucket_name,
                'Key': batch_json_key,
                'Body': json.dumps(batch_data, indent=2),
                'ContentType': 'application/json'
            }
            
            # Add conditional write parameter if we have an ETag
            if current_etag:
                put_object_args['IfMatch'] = current_etag
                logger.info(f"Attempt {attempt + 1}: Conditional write with ETag: {current_etag}")
            else:
                # For new files, ensure it doesn't exist
                put_object_args['IfNoneMatch'] = '*'
                logger.info(f"Attempt {attempt + 1}: Creating new file with IfNoneMatch=*")
            
            # Attempt the conditional write
            s3_client.put_object(**put_object_args)
            
            logger.info(f"Attempt {attempt + 1}: Successfully updated batch.json for {batch_folder}")
            
            # Success! Return the result
            return {
                'already_processed': False,
                'batch_json_updated': True,
                'patient_complete': patient_complete,
                'batch_complete': batch_data['status'] == 'complete',
                'batch_newly_completed': batch_newly_completed,
                'patient_file_counts': {patient_id: patient_data['file_counts'] for patient_id, patient in batch_data['patients'].items()},
                'batch_json_key': batch_json_key,
                'event_fingerprint': event_fingerprint,
                'retry_attempts': attempt,
                'version': batch_data['version'],
                'debug_info': {
                    'total_patients': total_patients,
                    'complete_patients': complete_patients,
                    'all_patients_meet_threshold': all_patients_meet_threshold,
                    'batch_completion_percentage': round(batch_completion_percentage, 2),
                    'completion_threshold': COMPLETION_THRESHOLD_PERCENTAGE
                }
            }
            
        except ClientError as e:
            # Handle conditional write failures (race conditions)
            if e.response['Error']['Code'] in ['PreconditionFailed', 'ConditionalRequestConflict']:
                # Another Lambda updated the file between our read and write
                retry_delay = min(BASE_RETRY_DELAY * (2 ** attempt) + random.uniform(0, 0.1), MAX_RETRY_DELAY)
                logger.warning(f"Attempt {attempt + 1}: Race condition detected, retrying in {retry_delay:.3f}s - {e.response['Error']['Code']}")
                
                if attempt < MAX_RETRY_ATTEMPTS - 1:  # Don't sleep on the last attempt
                    time.sleep(retry_delay)
                    continue  # Retry the operation
                else:
                    logger.error(f"Race condition: Max retry attempts ({MAX_RETRY_ATTEMPTS}) exceeded for {batch_json_key}")
                    return {
                        'batch_json_updated': False,
                        'error': f"Race condition: Max retry attempts exceeded after {MAX_RETRY_ATTEMPTS} tries",
                        'retry_attempts': attempt + 1,
                        'error_type': 'race_condition_max_retries'
                    }
            else:
                # Other S3 errors
                logger.error(f"Attempt {attempt + 1}: S3 error updating batch.json: {e.response['Error']['Code']} - {e.response['Error']['Message']}")
                return {
                    'batch_json_updated': False,
                    'error': f"S3 error: {e.response['Error']['Message']}",
                    'retry_attempts': attempt,
                    'error_type': 's3_error'
                }
        
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}: Unexpected error updating batch.json: {str(e)}")
            return {
                'batch_json_updated': False,
                'error': str(e),
                'retry_attempts': attempt,
                'error_type': 'unexpected_error'
            }
    
    # This should never be reached due to the logic above, but just in case
    return {
        'batch_json_updated': False,
        'error': 'Unknown error in race protection loop',
        'retry_attempts': MAX_RETRY_ATTEMPTS,
        'error_type': 'unknown_error'
    }

def parse_object_key(object_key: str) -> Dict[str, Any]:
    """
    Parse S3 object key to extract batch and patient information
    """
    try:
        logger.info(f"Parsing object key: {object_key}")
        
        path_parts = object_key.split('/')
        
        if len(path_parts) < 4:
            logger.warning(f"Not enough path parts: {len(path_parts)}, expected at least 4")
            return None
        
        batch_folder = path_parts[0]
        patient_id = path_parts[1]
        file_type = path_parts[2]
        filename = path_parts[3]
        
        if file_type not in EXPECTED_FILE_COUNTS:
            logger.warning(f"Unknown file type: {file_type}")
            return None
        
        if not all([batch_folder, patient_id, file_type]):
            logger.warning(f"Missing required fields")
            return None
        
        return {
            'batch_folder': batch_folder,
            'patient_id': patient_id,
            'file_type': file_type,
            'filename': filename
        }
    
    except Exception as e:
        logger.error(f"Error parsing object key {object_key}: {str(e)}")
        return None

def process_regular_s3_event(bucket_name: str, object_key: str, s3_client) -> Dict[str, Any]:
    """
    Process regular S3 events that don't match the batch pattern
    """
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        
        return {
            'success': True,
            'data': {
                'bucket': bucket_name,
                'key': object_key,
                'size': response.get('ContentLength'),
                'last_modified': response.get('LastModified').isoformat() if response.get('LastModified') else None,
                'type': 'regular_s3_event'
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': f"Regular S3 processing error: {str(e)}"
        }

def trigger_batch_complete_events(eventbridge_client, bucket_name: str, batch_info: Dict[str, Any], update_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Trigger EventBridge rules when a batch becomes complete
    """
    try:
        # Get EventBridge configuration from environment variables
        eventbridge_config = get_eventbridge_config()
        completion_rule_name = eventbridge_config['completion_rule_name']
        processing_rule_name = eventbridge_config['processing_rule_name']
        eventbus_name = eventbridge_config['bus_name']
        
        batch_folder = batch_info['batch_folder']
        completion_timestamp = datetime.utcnow().isoformat()
        
        # Event 1: Batch Processing Complete
        event_1 = {
            'Source': 'garmin.batch.processor',
            'DetailType': 'Batch Processing Complete',
            'Detail': json.dumps({
                'batchId': batch_folder,
                'bucketName': bucket_name,
                'completedAt': completion_timestamp,
                'totalPatients': len(update_result.get('patient_file_counts', {})),
                'status': 'complete',
                'eventType': 'batch_completion',
                'metadata': {
                    'batchFolder': batch_folder,
                    'triggerFile': f"{batch_info['patient_id']}/{batch_info['file_type']}/{batch_info['filename']}",
                    'eventFingerprint': batch_info.get('event_fingerprint'),
                    'completionReason': 'all_required_files_received',
                    'targetRule': completion_rule_name
                }
            }),
            'EventBusName': eventbus_name
        }
        
        # Event 2: Batch Ready for Processing
        event_2 = {
            'Source': 'garmin.data.pipeline',
            'DetailType': 'Batch Ready for Processing',
            'Detail': json.dumps({
                'batchId': batch_folder,
                'bucketName': bucket_name,
                'readyAt': completion_timestamp,
                'dataLocation': f"s3://{bucket_name}/{batch_folder}/",
                'processingStatus': 'ready',
                'patientCount': len(update_result.get('patient_file_counts', {})),
                'fileStatistics': {
                    'totalFiles': sum(
                        sum(patient_counts.values()) 
                        for patient_counts in update_result.get('patient_file_counts', {}).values()
                    ),
                    'expectedFiles': sum(EXPECTED_FILE_COUNTS.values()) * len(update_result.get('patient_file_counts', {})),
                    'fileTypeBreakdown': EXPECTED_FILE_COUNTS,
                    'patientFileBreakdown': update_result.get('patient_file_counts', {})
                },
                'metadata': {
                    'batchFolder': batch_folder,
                    'batchJsonLocation': f"s3://{bucket_name}/{batch_folder}/batch.json",
                    'lastFileProcessed': f"{batch_info['patient_id']}/{batch_info['file_type']}/{batch_info['filename']}",
                    'processingTrigger': 'file_completion_threshold_met',
                    'targetRule': processing_rule_name
                }
            }),
            'EventBusName': eventbus_name
        }
        
        # Send both events to EventBridge
        events_to_send = [event_1, event_2]
        
        response = eventbridge_client.put_events(Entries=events_to_send)
        
        # Check for any failed events
        failed_events = response.get('FailedEntryCount', 0)
        
        if failed_events > 0:
            logger.error(f"Failed to send {failed_events} events to EventBridge")
            logger.error(f"Failed entries: {response.get('Entries', [])}")
        else:
            logger.info(f"Successfully sent 2 different events to EventBridge for batch {batch_folder}")
            logger.info(f"Event 1: Targeting rule '{completion_rule_name}'")
            logger.info(f"Event 2: Targeting rule '{processing_rule_name}'")
        
        return {
            'events_sent': len(events_to_send),
            'failed_events': failed_events,
            'eventbridge_response': response,
            'targeted_rules': [
                completion_rule_name,
                processing_rule_name
            ],
            'event_details': [
                {
                    'event_number': 1,
                    'source': 'garmin.batch.processor',
                    'detail_type': 'Batch Processing Complete',
                    'batch_id': batch_folder,
                    'purpose': 'Completion notification',
                    'target_rule': completion_rule_name
                },
                {
                    'event_number': 2,
                    'source': 'garmin.data.pipeline', 
                    'detail_type': 'Batch Ready for Processing',
                    'batch_id': batch_folder,
                    'purpose': 'Processing trigger',
                    'target_rule': processing_rule_name
                }
            ]
        }
        
    except Exception as e:
        logger.error(f"Error triggering EventBridge events: {str(e)}")
        return {
            'error': str(e),
            'events_sent': 0,
            'failed_events': len(events_to_send) if 'events_to_send' in locals() else 2
        }

def handle_failed_messages(failed_messages: List[Dict[str, Any]]) -> None:
    """
    Handle failed messages with enhanced logging for race condition analysis
    """
    try:
        logger.error(f"Failed to process {len(failed_messages)} messages")
        
        race_condition_failures = 0
        s3_failures = 0
        other_failures = 0
        
        for failed_msg in failed_messages:
            logger.error(f"Failed message: {failed_msg}")
            
            # Categorize failure types for monitoring
            error_type = failed_msg.get('error_type', 'unknown')
            if error_type == 'race_condition_max_retries':
                race_condition_failures += 1
            elif error_type == 's3_error':
                s3_failures += 1
            else:
                other_failures += 1
        
        # Log failure summary for monitoring/alerting
        if race_condition_failures > 0:
            logger.error(f"ALERT: {race_condition_failures} messages failed due to persistent race conditions!")
        if s3_failures > 0:
            logger.error(f"ALERT: {s3_failures} messages failed due to S3 errors!")
        if other_failures > 0:
            logger.error(f"ALERT: {other_failures} messages failed due to other errors!")
            
    except Exception as e:
        logger.error(f"Error handling failed messages: {str(e)}")