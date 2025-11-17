import json
import os
import boto3
import pandas as pd
from io import StringIO
import re
from datetime import datetime

def extract_participantId(object_key):
    """
    Extracts the participant ID from the object key
    """
    match = re.search(r'_([a-z0-9]{8})\.csv$', object_key)
    return match.group(1) if match else None

def format_timestamp_from_iso(iso_date):
    """
    Formats ISO date string to yyyy-mm-dd hh:mm format (removing seconds)
    """
    try:
        # Parse the ISO date string
        dt = datetime.fromisoformat(iso_date.replace('Z', '+00:00'))
        # Format with just minutes, no seconds
        return dt.strftime('%Y-%m-%d %H:%M')
    except (ValueError, AttributeError):
        # Return original value if parsing fails
        return iso_date

def process_step_data(df, participant_id):
    """
    Processes step data and returns only unique rows
    """
    # Add participant ID
    df['participant_id'] = participant_id
    
    # Add timestamp column from isoDate before dropping it
    if 'isoDate' in df.columns:
        df.insert(
            df.columns.get_loc('isoDate') + 1,
            'Timestamp',
            df['isoDate'].apply(format_timestamp_from_iso)
        )
        
        # Add the new column: participantid_timestamp
        df['participantid_timestamp'] = df['participant_id'] + '_' + df['Timestamp'].astype(str)
    
    # Delete specified columns - modified to keep isoDate and unixTimestampInMs
    columns_to_drop = ['durationInMs', 'deviceType', 'timezone', 'isoDate']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
    
    # Reorder columns to make participant_id first
    cols = ['participant_id'] + [col for col in df.columns if col != 'participant_id']
    df = df[cols]
    
    # Drop duplicate rows to keep only unique entries
    df_before = len(df)
    df = df.drop_duplicates()
    df_after = len(df)
    
    # Log the number of duplicates removed
    duplicates_removed = df_before - df_after
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} duplicate rows")
    
    return df

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')
    destination_bucket = os.environ.get('DESTINATION_BUCKET')
    
    processed_files = []
    failed_files = []

    for record in event['Records']:
        object_key = None  # Initialize to avoid NameError
        try:
            # Parse message body
            message = json.loads(record['body'])
            source_bucket = message['source_bucket']
            object_key = message['object_key']

            print(f"Processing file: s3://{source_bucket}/{object_key}")

            # Extract participant ID
            participant_id = extract_participantId(object_key)
            if not participant_id:
                raise ValueError(f"Could not extract participant ID from {object_key}")

            # Read file from S3
            response = s3.get_object(Bucket=source_bucket, Key=object_key)
            file_content = response['Body'].read().decode('utf-8')

            # Parse CSV
            df = pd.read_csv(StringIO(file_content), skiprows=6, encoding='utf-8')
            print(f"Original columns: {df.columns.tolist()}")
            print(f"Original row count: {len(df)}")
            
            # Process data and keep only unique rows
            processed_df = process_step_data(df, participant_id)
            print(f"Processed columns: {processed_df.columns.tolist()}")
            print(f"Processed row count (after deduplication): {len(processed_df)}")

            # Convert to CSV
            output_buffer = StringIO()
            processed_df.to_csv(output_buffer, index=False)

            # Upload processed file to destination
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
            failed_files.append({
                'file': object_key if object_key else 'unknown',
                'error': str(e)
            })

    # Prepare the response
    response = {
        'statusCode': 200,
        'body': json.dumps({
            'processed_files': len(processed_files),
            'failed_files': len(failed_files),
            'failures': failed_files if failed_files else None,
            'processed': processed_files if processed_files else None,
            'message': 'Processed by HealthHeartRateFunction_DEV'
        }, indent=2)
    }

    # Log the complete response
    print("Lambda Execution Summary:")
    print("-" * 50)
    print(f"Total Files Processed: {len(processed_files)}")
    print(f"Total Files Failed: {len(failed_files)}")
    
    if processed_files:
        print("\nSuccessfully Processed Files:")
        for file in processed_files:
            print(f"✓ {file['file']}")
            print(f"  Original rows: {file['original_rows']}")
            print(f"  Unique rows: {file['unique_rows']}")
            print(f"  Duplicates removed: {file['duplicates_removed']}")
    
    if failed_files:
        print("\nFailed Files:")
        for file in failed_files:
            print(f"✗ {file['file']}")
            print(f"  Error: {file['error']}")
    
    print("\nComplete Response:")
    print(json.dumps(response, indent=2))
    print("-" * 50)

    return response