import json
import os
import boto3
import logging
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Garmin data-type → DynamoDB attribute names ──────────────────────────────
# actual count attribute   = how many processed files have arrived so far
# expected count attribute = how many files exist in the SOURCE bucket for this type

FILE_TYPE_TO_ATTR = {
    'garmin-device-heart-rate':   'heart_rate_count',
    'garmin-device-stress':       'stress_count',
    'garmin-device-step':         'step_count',
    'garmin-device-respiration':  'respiration_count',
    'garmin-device-pulse-ox':     'pulse_ox_count',
    'garmin-connect-sleep-stage': 'sleep_count',
}

FILE_TYPE_TO_EXPECTED_ATTR = {
    'garmin-device-heart-rate':   'expected_heart_rate_count',
    'garmin-device-stress':       'expected_stress_count',
    'garmin-device-step':         'expected_step_count',
    'garmin-device-respiration':  'expected_respiration_count',
    'garmin-device-pulse-ox':     'expected_pulse_ox_count',
    'garmin-connect-sleep-stage': 'expected_sleep_count',
}

ALL_DATA_TYPES = list(FILE_TYPE_TO_ATTR.keys())


def get_eventbridge_config() -> Dict[str, str]:
    return {
        'bus_name':             os.environ.get('EVENTBRIDGE_BUS_NAME', 'default'),
        'garmin_detail_type':   os.environ.get('EVENTBRIDGE_GARMIN_DETAIL_TYPE', 'AlbanyHealthGarminHealthMetricsWorkflow'),
        'bbi_detail_type':      os.environ.get('EVENTBRIDGE_BBI_DETAIL_TYPE', 'AlbanyHealthGarminBBIWorkflow'),
        'completion_rule_name': os.environ.get('EVENTBRIDGE_COMPLETION_RULE_NAME', 'trigger-merge-patient-health-metrics'),
        'processing_rule_name': os.environ.get('EVENTBRIDGE_PROCESSING_RULE_NAME', 'trigger-patients-bbi-flow'),
    }


# ── Lambda entry point ───────────────────────────────────────────────────────

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    table_name    = os.environ['BATCH_TRACKING_TABLE']
    source_bucket = os.environ['SOURCE_BUCKET']

    dynamodb   = boto3.client('dynamodb')
    s3         = boto3.client('s3')
    eventbridge = boto3.client('events')

    processed, skipped, failed = [], [], []

    logger.info(f"Received {len(event['Records'])} SQS messages")

    for record in event['Records']:
        message_id = record.get('messageId', 'unknown')
        try:
            message_body = json.loads(record['body'])
            s3_record = extract_s3_record(message_body)

            if not s3_record:
                logger.warning(f"Message {message_id}: no S3 record found, skipping")
                skipped.append(message_id)
                continue

            bucket_name, object_key = extract_s3_coordinates(s3_record)
            if not bucket_name or not object_key:
                logger.warning(f"Message {message_id}: missing bucket/key, skipping")
                skipped.append(message_id)
                continue

            batch_info = parse_object_key(object_key)
            if not batch_info:
                logger.info(f"Message {message_id}: key {object_key!r} does not match batch pattern, skipping")
                skipped.append(message_id)
                continue

            result = process_file(dynamodb, s3, eventbridge, table_name, source_bucket, batch_info)

            if result == 'duplicate':
                skipped.append(message_id)
            elif result == 'triggered':
                processed.append({'messageId': message_id, 'batch_triggered': True})
            else:
                processed.append({'messageId': message_id, 'batch_triggered': False})

        except Exception as e:
            logger.error(f"Message {message_id}: unexpected error — {e}", exc_info=True)
            failed.append({'messageId': message_id, 'error': str(e)})

    logger.info(f"Done. processed={len(processed)}, skipped={len(skipped)}, failed={len(failed)}")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'processedCount': len(processed),
            'skippedCount':   len(skipped),
            'failedCount':    len(failed),
            'processed':      processed,
            'failed':         failed,
            'timestamp':      datetime.now(timezone.utc).isoformat(),
        })
    }


# ── Core processing logic ────────────────────────────────────────────────────

def process_file(
    dynamodb_client,
    s3_client,
    eventbridge_client,
    table_name: str,
    source_bucket: str,
    batch_info: Dict[str, Any],
) -> str:
    """
    Atomically record the processed file in DynamoDB.

    Expected counts are NOT hardcoded — they are read dynamically from the
    source bucket (albanyhealthsource-s3bucket-{env}) by counting how many
    CSV files exist under {patient_id}/{data_type}/.

    Returns: 'duplicate' | 'triggered' | 'counted'
    """
    patient_id = batch_info['patient_id']
    file_type  = batch_info['file_type']
    filename   = batch_info['filename']
    batch_date = batch_info['batch_date']
    file_id    = f"{file_type}/{filename}"

    # Append batch_date to patient_id so each weekly submission gets its own row
    # without needing a sort key. e.g. "Testing-1_cd037752#2025-02-04"
    db_key = {'patient_id': {'S': f"{patient_id}#{batch_date}"}}

    count_attr    = FILE_TYPE_TO_ATTR[file_type]
    expected_attr = FILE_TYPE_TO_EXPECTED_ATTR[file_type]
    now = datetime.now(timezone.utc).isoformat()

    # ── Step 1: Get expected count — DynamoDB first, S3 only if not yet stored ─
    # On the first processed file of a given type for this patient, the expected
    # count has not been stored yet so we query S3.  On every subsequent file of
    # the same type the value is already in DynamoDB, so we skip the S3 call.
    pre_check = dynamodb_client.get_item(
        TableName=table_name,
        Key=db_key,
        ConsistentRead=False,
    )
    existing_item = pre_check.get('Item', {})

    stored_expected = int(existing_item.get(expected_attr, {}).get('N', '0'))

    if stored_expected > 0:
        # Expected count for this metric already stored — no need to query S3
        source_count = stored_expected
        logger.info(f"Expected count for {file_type}: {source_count} (from DynamoDB, skipping S3)")
    else:
        # First file of this metric for this patient — query S3 to get the expected count
        source_count = count_source_files(s3_client, source_bucket, patient_id, file_type)
        if source_count == 0:
            logger.warning(
                f"No source files found for {patient_id}/{file_type} in {source_bucket}. "
                f"Expected count will remain unset for this type."
            )

    # ── Step 2: Atomic write — count the processed file, set expected once ───
    #
    # The UpdateExpression does three things in a single atomic call:
    #   SET  — initialise status/created_at on the very first write for this
    #          patient; set expected_<type>_count once (if_not_exists so it is
    #          never overwritten by a later, possibly lower, source count)
    #   ADD  — atomically increment the per-type counter, total_files, and
    #          append file_id to the StringSet for deduplication
    #
    # The ConditionExpression rejects the write if this file_id was already
    # counted, giving us free atomic deduplication.
    try:
        update_expr = (
            'SET #st          = if_not_exists(#st, :processing), '
            '    created_at   = if_not_exists(created_at, :now), '
            '    last_updated = :now '
        )
        expr_attr_names = {
            '#st':  'status',
            '#cnt': count_attr,
        }
        expr_attr_values = {
            ':one':        {'N': '1'},
            ':file_set':   {'SS': [file_id]},
            ':file_id':    {'S': file_id},
            ':processing': {'S': 'processing'},
            ':now':        {'S': now},
        }

        # Only store expected count if we actually found source files
        if source_count > 0:
            update_expr += ', #exp_cnt = if_not_exists(#exp_cnt, :source_count) '
            expr_attr_names['#exp_cnt'] = expected_attr
            expr_attr_values[':source_count'] = {'N': str(source_count)}

        update_expr += 'ADD #cnt :one, total_files :one, processed_files :file_set'

        dynamodb_client.update_item(
            TableName=table_name,
            Key=db_key,
            UpdateExpression=update_expr,
            ConditionExpression='NOT contains(processed_files, :file_id)',
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
        )
        logger.info(
            f"Counted {file_id} for {patient_id} "
            f"(source_count={source_count})"
        )

    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.info(f"Duplicate: {file_id} for {patient_id} already counted")
            return 'duplicate'
        raise

    # ── Step 3: Read back current state to check batch completion ────────────
    response = dynamodb_client.get_item(
        TableName=table_name,
        Key=db_key,
        ConsistentRead=True,
    )
    item = response.get('Item', {})

    if not is_batch_complete(item):
        log_progress(patient_id, item)
        return 'counted'

    # ── Step 4: Atomically claim the "trigger" role ───────────────────────────
    # Of all the concurrent Lambda invocations that reach this point,
    # only one wins this conditional write and fires EventBridge.
    try:
        dynamodb_client.update_item(
            TableName=table_name,
            Key=db_key,
            UpdateExpression='SET #st = :complete, completed_at = :now',
            ConditionExpression='#st = :processing',
            ExpressionAttributeNames={'#st': 'status'},
            ExpressionAttributeValues={
                ':complete':   {'S': 'complete'},
                ':processing': {'S': 'processing'},
                ':now':        {'S': now},
            },
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.info(
                f"Batch {patient_id} already marked complete by another invocation — "
                f"skipping EventBridge"
            )
            return 'counted'
        raise

    # ── Step 5: Trigger EventBridge ───────────────────────────────────────────
    logger.info(f"Batch {patient_id} complete — triggering EventBridge")
    trigger_eventbridge(eventbridge_client, patient_id, item)
    return 'triggered'


# ── Source bucket counting ────────────────────────────────────────────────────

def count_source_files(s3_client, source_bucket: str, patient_id: str, file_type: str) -> int:
    """
    Count how many CSV files exist in:
        s3://{source_bucket}/{patient_id}/{file_type}/

    This is the ground-truth expected count for this patient + data type.
    Uses pagination to handle more than 1000 files.
    """
    prefix = f"{patient_id}/{file_type}/"
    count = 0

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=source_bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                count += 1

    logger.info(
        f"Source bucket count — {source_bucket}/{prefix} : {count} file(s)"
    )
    return count


# ── Completion check ──────────────────────────────────────────────────────────

def is_batch_complete(item: Dict[str, Any]) -> bool:
    """
    Batch is complete when EVERY data type that has source files
    (expected_<type>_count > 0) has been fully processed
    (actual_<type>_count >= expected_<type>_count).

    Types where expected = 0 (or not yet set) are skipped — they were
    not part of this patient's batch.

    At least one type must be present for the batch to be considered complete.
    """
    has_any_type = False

    for file_type in ALL_DATA_TYPES:
        count_attr    = FILE_TYPE_TO_ATTR[file_type]
        expected_attr = FILE_TYPE_TO_EXPECTED_ATTR[file_type]

        expected = int(item.get(expected_attr, {}).get('N', '0'))

        if expected == 0:
            # This data type was not part of this patient's batch
            continue

        has_any_type = True
        actual = int(item.get(count_attr, {}).get('N', '0'))

        if actual < expected:
            return False

    return has_any_type


def log_progress(patient_id: str, item: Dict[str, Any]) -> None:
    """Log per-type actual vs expected counts for debugging."""
    counts = {}
    for file_type in ALL_DATA_TYPES:
        count_attr    = FILE_TYPE_TO_ATTR[file_type]
        expected_attr = FILE_TYPE_TO_EXPECTED_ATTR[file_type]
        actual   = int(item.get(count_attr, {}).get('N', '0'))
        expected = int(item.get(expected_attr, {}).get('N', '0'))
        if expected > 0:
            counts[file_type] = f"{actual}/{expected}"

    total = int(item.get('total_files', {}).get('N', '0'))
    logger.info(f"Progress for {patient_id}: {total} file(s) processed — {counts}")


# ── EventBridge trigger ───────────────────────────────────────────────────────

def trigger_eventbridge(eventbridge_client, patient_id: str, item: Dict[str, Any]) -> None:
    cfg = get_eventbridge_config()
    now = datetime.now(timezone.utc).isoformat()

    base = {}
    if cfg['bus_name'] and cfg['bus_name'] != 'default':
        base['EventBusName'] = cfg['bus_name']

    detail_common = {
        'batchId':    patient_id,
        'completedAt': now,
        'status':     'complete',
        'totalFiles': int(item.get('total_files', {}).get('N', '0')),
        'metadata': {
            'batchFolder': patient_id,
        }
    }

    events = [
        {
            'Source':     'lambda',
            'DetailType': cfg['garmin_detail_type'],
            'Detail':     json.dumps({**detail_common, 'targetRule': cfg['completion_rule_name']}),
            **base,
        },
        {
            'Source':     'lambda',
            'DetailType': cfg['bbi_detail_type'],
            'Detail':     json.dumps({**detail_common, 'targetRule': cfg['processing_rule_name']}),
            **base,
        },
    ]

    response = eventbridge_client.put_events(Entries=events)
    failed_count = response.get('FailedEntryCount', 0)
    if failed_count:
        logger.error(
            f"EventBridge: {failed_count} event(s) failed for batch {patient_id}: "
            f"{response['Entries']}"
        )
    else:
        logger.info(
            f"EventBridge: sent 2 events for batch {patient_id} — "
            f"garmin={cfg['garmin_detail_type']}, bbi={cfg['bbi_detail_type']}"
        )


# ── S3 / SQS parsing helpers ──────────────────────────────────────────────────

def extract_s3_record(message_body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Pull the first S3 record out of a standard S3-event-via-SQS message."""
    if 'Records' in message_body:
        for rec in message_body['Records']:
            if rec.get('eventSource') == 'aws:s3':
                return rec
    elif message_body.get('eventSource') == 'aws:s3':
        return message_body
    return None


def extract_s3_coordinates(s3_record: Dict[str, Any]):
    s3_info = s3_record.get('s3', {})
    bucket  = s3_info.get('bucket', {}).get('name')
    key     = s3_info.get('object', {}).get('key')
    if key:
        key = urllib.parse.unquote_plus(key)
    return bucket, key


def parse_object_key(object_key: str) -> Optional[Dict[str, Any]]:
    """
    Expected format: {patient_id}/{file_type}/{filename}
    e.g. Testing-1_cd037752/garmin-device-heart-rate/250204_garmin-device-heart-rate_Testing-1_cd037752.csv

    Also extracts batch_date from the YYMMDD prefix in the filename so that
    the DynamoDB composite key (patient_id, batch_date) is unique per submission.
    """
    parts = object_key.split('/')
    if len(parts) < 3:
        return None

    patient_id = parts[0]
    file_type  = parts[1]
    filename   = parts[2]

    if file_type not in FILE_TYPE_TO_ATTR:
        logger.debug(f"Unknown file_type {file_type!r} in key {object_key!r}")
        return None

    # Use today's UTC date so the primary key is unique per patient per day
    batch_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    return {
        'patient_id': patient_id,
        'file_type':  file_type,
        'filename':   filename,
        'batch_date': batch_date,
    }
