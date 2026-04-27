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

# Survey data type folder in the source bucket
# Path format: {patient_id}/questionnaire/{filename}
SURVEY_DATA_TYPE       = 'questionnaire'
ACTUAL_COUNT_ATTR      = 'questionnaire_count'
EXPECTED_COUNT_ATTR    = 'expected_questionnaire_count'


def get_eventbridge_config() -> Dict[str, str]:
    return {
        'bus_name':           os.environ.get('EVENTBRIDGE_BUS_NAME', 'default'),
        'survey_detail_type': os.environ.get('EVENTBRIDGE_SURVEY_DETAIL_TYPE', 'AlbanyHealthSurveyDataMergedFiles'),
        'survey_rule_name':   os.environ.get('EVENTBRIDGE_SURVEY_RULE_NAME', 'trigger-survey-data-merged-files'),
    }


# ── Lambda entry point ───────────────────────────────────────────────────────

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    table_name          = os.environ['SURVEY_BATCH_TRACKING_TABLE']
    processing_bucket   = os.environ['SURVEY_PROCESSING_BUCKET']

    dynamodb    = boto3.client('dynamodb')
    s3          = boto3.client('s3')
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
                logger.info(f"Message {message_id}: key {object_key!r} does not match survey pattern, skipping")
                skipped.append(message_id)
                continue

            result = process_file(dynamodb, s3, eventbridge, table_name, processing_bucket, batch_info)

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
    processing_bucket: str,
    batch_info: Dict[str, Any],
) -> str:
    """
    Atomically record the processed survey file in DynamoDB.

    Source path format : {patient_id}/questionnaire/{filename}
    Expected count     : number of files in source_bucket/{patient_id}/questionnaire/
    DynamoDB key       : patient_id = "{patient_id}#{batch_date}"  e.g. "Testing-1_cd037752#2025-04-27"

    Returns: 'duplicate' | 'triggered' | 'counted'
    """
    patient_id = batch_info['patient_id']
    filename   = batch_info['filename']
    batch_date = batch_info['batch_date']
    file_id    = f"{SURVEY_DATA_TYPE}/{filename}"

    db_key = {'patient_id': {'S': f"{patient_id}#{batch_date}"}}
    now    = datetime.now(timezone.utc).isoformat()

    # ── Step 1: Get expected count — DynamoDB first, S3 only if not yet stored ─
    pre_check     = dynamodb_client.get_item(TableName=table_name, Key=db_key, ConsistentRead=False)
    existing_item = pre_check.get('Item', {})
    stored_expected = int(existing_item.get(EXPECTED_COUNT_ATTR, {}).get('N', '0'))

    if stored_expected > 0:
        source_count = stored_expected
        logger.info(f"Expected count for {patient_id}/questionnaire: {source_count} (from DynamoDB, skipping S3)")
    else:
        source_count = count_source_files(s3_client, processing_bucket, patient_id)
        if source_count == 0:
            logger.warning(
                f"No source files found for {patient_id}/questionnaire in {processing_bucket}. "
                f"Expected count will remain unset."
            )

    # ── Step 2: Atomic write ──────────────────────────────────────────────────
    try:
        update_expr = (
            'SET #st        = if_not_exists(#st, :processing), '
            '    created_at = if_not_exists(created_at, :now), '
            '    last_updated = :now '
        )
        expr_attr_names  = {'#st': 'status', '#cnt': ACTUAL_COUNT_ATTR}
        expr_attr_values = {
            ':one':        {'N': '1'},
            ':file_set':   {'SS': [file_id]},
            ':file_id':    {'S': file_id},
            ':processing': {'S': 'processing'},
            ':now':        {'S': now},
        }

        if source_count > 0:
            update_expr += ', #exp_cnt = if_not_exists(#exp_cnt, :source_count) '
            expr_attr_names['#exp_cnt']         = EXPECTED_COUNT_ATTR
            expr_attr_values[':source_count']   = {'N': str(source_count)}

        update_expr += 'ADD #cnt :one, total_files :one, processed_files :file_set'

        dynamodb_client.update_item(
            TableName=table_name,
            Key=db_key,
            UpdateExpression=update_expr,
            ConditionExpression='NOT contains(processed_files, :file_id)',
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
        )
        logger.info(f"Counted {file_id} for {patient_id} (source_count={source_count})")

    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.info(f"Duplicate: {file_id} for {patient_id} already counted")
            return 'duplicate'
        raise

    # ── Step 3: Read back and check completion ────────────────────────────────
    response = dynamodb_client.get_item(TableName=table_name, Key=db_key, ConsistentRead=True)
    item     = response.get('Item', {})

    if not is_batch_complete(item):
        log_progress(patient_id, item)
        return 'counted'

    # ── Step 4: Atomically claim the trigger role ─────────────────────────────
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
            logger.info(f"Survey batch {patient_id} already marked complete — skipping EventBridge")
            return 'counted'
        raise

    # ── Step 5: Fire survey EventBridge rule ──────────────────────────────────
    logger.info(f"Survey batch {patient_id} complete — triggering EventBridge")
    trigger_eventbridge(eventbridge_client, patient_id, item)
    return 'triggered'


# ── Completion check ──────────────────────────────────────────────────────────

def is_batch_complete(item: Dict[str, Any]) -> bool:
    expected = int(item.get(EXPECTED_COUNT_ATTR, {}).get('N', '0'))
    if expected == 0:
        return False
    actual = int(item.get(ACTUAL_COUNT_ATTR, {}).get('N', '0'))
    return actual >= expected


def log_progress(patient_id: str, item: Dict[str, Any]) -> None:
    actual   = int(item.get(ACTUAL_COUNT_ATTR,   {}).get('N', '0'))
    expected = int(item.get(EXPECTED_COUNT_ATTR, {}).get('N', '0'))
    logger.info(f"Survey progress for {patient_id}: {actual}/{expected} questionnaire file(s)")


# ── Source bucket counting ────────────────────────────────────────────────────

def count_source_files(s3_client, processing_bucket: str, patient_id: str) -> int:
    """
    Count CSV/JSON files under s3://{processing_bucket}/{patient_id}/questionnaire/
    This is the expected number of processed survey files for this patient.
    """
    prefix = f"{patient_id}/{SURVEY_DATA_TYPE}/"
    count  = 0
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=processing_bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') or key.endswith('.json'):
                count += 1
    logger.info(f"Processing bucket count — {processing_bucket}/{prefix}: {count} file(s)")
    return count


# ── EventBridge trigger ───────────────────────────────────────────────────────

def trigger_eventbridge(eventbridge_client, patient_id: str, item: Dict[str, Any]) -> None:
    cfg = get_eventbridge_config()
    now = datetime.now(timezone.utc).isoformat()

    base = {}
    if cfg['bus_name'] and cfg['bus_name'] != 'default':
        base['EventBusName'] = cfg['bus_name']

    event = {
        'Source':     'lambda',
        'DetailType': cfg['survey_detail_type'],
        'Detail': json.dumps({
            'batchId':    patient_id,
            'completedAt': now,
            'status':     'complete',
            'totalFiles': int(item.get('total_files', {}).get('N', '0')),
            'metadata': {
                'batchFolder': patient_id,
                'targetRule':  cfg['survey_rule_name'],
            }
        }),
        **base,
    }

    response    = eventbridge_client.put_events(Entries=[event])
    failed_count = response.get('FailedEntryCount', 0)
    if failed_count:
        logger.error(f"EventBridge: event failed for survey batch {patient_id}: {response['Entries']}")
    else:
        logger.info(
            f"EventBridge: survey event sent for {patient_id} — "
            f"detail_type={cfg['survey_detail_type']}"
        )


# ── S3 / SQS parsing helpers ──────────────────────────────────────────────────

def extract_s3_record(message_body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
    Expected format: {patient_id}/questionnaire/{filename}
    e.g. Testing-1_cd037752/questionnaire/Physical-Capacity-Aw_6274612c
    """
    parts = object_key.split('/')
    if len(parts) < 3:
        return None

    patient_id = parts[0]
    data_type  = parts[1]
    filename   = parts[2]

    if data_type != SURVEY_DATA_TYPE:
        logger.debug(f"Skipping non-survey path: {object_key!r}")
        return None

    batch_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    return {
        'patient_id': patient_id,
        'filename':   filename,
        'batch_date': batch_date,
    }