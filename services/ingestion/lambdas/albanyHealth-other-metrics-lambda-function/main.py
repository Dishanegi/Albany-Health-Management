import json
import os
import boto3
import pandas as pd
from io import StringIO
import re
from datetime import datetime

def extract_participant_id(object_key):
    """
    Extracts the participant ID from the object key
    """
    match = re.search(r'_([a-z0-9]{8})\.csv$', object_key)
    return match.group(1) if match else None

def format_iso_timestamp(iso_date_string):
    """
    Formats ISO date string to yyyy-mm-dd hh:mm format (zero seconds)
    """
    try:
        # Parse the ISO date string
        dt = datetime.fromisoformat(iso_date_string.replace('Z', '+00:00'))
        # Format with just minutes, no seconds
        return dt.strftime('%Y-%m-%d %H:%M')
    except Exception as e:
        print(f"Error formatting ISO date: {str(e)}")
        return None

def process_other_data(df, participant_id):
    """
    Processes other types of health data
    Applies basic transformations while preserving original data structure
    """
    # Add participant ID
    df['participant_id'] = participant_id
    
    # Add timestamp column if unixTimestampInMs exists
    if 'unixTimestampInMs' in df.columns and 'isoDate' in df.columns:
        df.insert(
            df.columns.get_loc('unixTimestampInMs') + 1,
            'Timestamp',
            df['isoDate'].apply(format_iso_timestamp)
        )
    
    # Add new participantid_timestamp column using the formatted timestamp
    df['participantid_timestamp'] = df['participant_id'] + '_' + df['Timestamp'].astype(str)
    
    # Remove specified columns if they exist
    columns_to_drop = ['timezone', 'isoDate', 'deviceType']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
    
    # Reorder columns to make participant_id first
    cols = ['participant_id'] + [col for col in df.columns if col != 'participant_id']
    return df[cols]

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')
    destination_bucket = os.environ.get('DESTINATION_BUCKET')
    
    processed_files = []
    failed_files = []

    for record in event['Records']:
        try:
            # Parse message body
            message = json.loads(record['body'])
            source_bucket = message['source_bucket']
            object_key = message['object_key']
            folder_name = message.get('folder_name', 'unknown')

            print(f"Processing file from folder {folder_name}: s3://{source_bucket}/{object_key}")

            # Extract participant ID
            participant_id = extract_participant_id(object_key)
            if not participant_id:
                raise ValueError(f"Could not extract participant ID from {object_key}")

            # Read file from S3
            response = s3.get_object(Bucket=source_bucket, Key=object_key)
            file_content = response['Body'].read().decode('utf-8')

            # Parse CSV - Skip first 6 rows
            df = pd.read_csv(StringIO(file_content), skiprows=6, encoding='utf-8')
            print(f"Original columns: {df.columns.tolist()}")
            
            # Process data
            processed_df = process_other_data(df, participant_id)
            print(f"Processed columns: {processed_df.columns.tolist()}")

            # Convert to CSV
            output_buffer = StringIO()
            processed_df.to_csv(output_buffer, index=False)

            # Upload to destination
            s3.put_object(
                Bucket=destination_bucket,
                Key=object_key,
                Body=output_buffer.getvalue(),
                ContentType='text/csv'
            )

            processed_files.append({
                'file': object_key,
                'folder': folder_name
            })
            print(f"Successfully processed: {object_key}")

        except Exception as e:
            print(f"Error processing file: {str(e)}")
            failed_files.append({
                'file': object_key if 'object_key' in locals() else 'unknown',
                'folder': folder_name if 'folder_name' in locals() else 'unknown',
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
            'message': 'Processed by HealthOthersFunction_DEV'
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
            print(f"✓ {file}")
    
    if failed_files:
        print("\nFailed Files:")
        for file in failed_files:
            print(f"✗ {file['file']}")
            print(f"  Error: {file['error']}")
    
    print("\nComplete Response:")
    print(json.dumps(response, indent=2))
    print("-" * 50)

    return response