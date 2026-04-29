import json
import os
import boto3
import pandas as pd
from io import StringIO

from utils import extract_participant_id, format_iso_timestamp, write_placeholder


def process_step_data(df, participant_id):
    df['participant_id'] = participant_id

    if 'isoDate' in df.columns:
        df.insert(
            df.columns.get_loc('isoDate') + 1,
            'Timestamp',
            df['isoDate'].apply(format_iso_timestamp)
        )
        df['participantid_timestamp'] = df['participant_id'] + '_' + df['Timestamp'].astype(str)

    columns_to_drop = ['durationInMs', 'deviceType', 'timezone', 'isoDate']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')

    cols = ['participant_id'] + [col for col in df.columns if col != 'participant_id']
    df = df[cols]

    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed > 0:
        print(f"Removed {removed} duplicate rows")

    return df


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    destination_bucket = os.environ.get('DESTINATION_BUCKET')

    processed_files = []
    failed_files = []

    for record in event['Records']:
        object_key = None
        try:
            message = json.loads(record['body'])
            source_bucket = message['source_bucket']
            object_key = message['object_key']

            print(f"Processing file: s3://{source_bucket}/{object_key}")

            participant_id = extract_participant_id(object_key)
            if not participant_id:
                raise ValueError(f"Could not extract participant ID from {object_key}")

            response = s3.get_object(Bucket=source_bucket, Key=object_key)
            file_content = response['Body'].read().decode('utf-8')

            df = pd.read_csv(StringIO(file_content), skiprows=6, encoding='utf-8')
            if df.empty:
                raise ValueError(f"File contains no data rows after skipping header: {object_key}")
            print(f"Original rows: {len(df)}, columns: {df.columns.tolist()}")

            processed_df = process_step_data(df, participant_id)
            print(f"Processed rows: {len(processed_df)}, columns: {processed_df.columns.tolist()}")

            output_buffer = StringIO()
            processed_df.to_csv(output_buffer, index=False)

            s3.put_object(
                Bucket=destination_bucket,
                Key=object_key,
                Body=output_buffer.getvalue(),
                ContentType='text/csv'
            )

            processed_files.append({
                'file': object_key,
                'original_rows': len(df),
                'unique_rows': len(processed_df),
                'duplicates_removed': len(df) - len(processed_df)
            })
            print(f"Successfully processed: {object_key}")

        except Exception as e:
            print(f"Error processing file: {str(e)}")
            if object_key and destination_bucket:
                write_placeholder(s3, destination_bucket, object_key)
            failed_files.append({'file': object_key or 'unknown', 'error': str(e)})

    print(f"Processed: {len(processed_files)}  Failed: {len(failed_files)}")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed_files': len(processed_files),
            'failed_files': len(failed_files),
            'failures': failed_files if failed_files else None,
            'processed': processed_files if processed_files else None,
        })
    }