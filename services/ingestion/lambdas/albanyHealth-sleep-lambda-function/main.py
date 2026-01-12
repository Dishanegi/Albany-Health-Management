import json
import os
import boto3
import pandas as pd
import numpy as np
from io import StringIO
import re
from datetime import datetime, timedelta

def extract_participantId(object_key):
    """
    Extracts the participant ID from the object key
    """
    match = re.search(r'_([a-z0-9]{8})\.csv$', object_key)
    return match.group(1) if match else None

def format_iso_date_to_timestamp(iso_date_str):
    """
    Formats ISO date string to yyyy-mm-dd hh:mm format (with seconds set to zero)
    """
    # Parse the ISO date string
    dt = datetime.fromisoformat(iso_date_str.replace('Z', '+00:00'))
    # Format with just minutes, no seconds
    return dt.strftime('%Y-%m-%d %H:%M')

def convert_ms_to_minutes(duration_ms):
    """
    Converts duration in milliseconds to minutes
    """
    return duration_ms / (1000 * 60)

def expand_sleep_data(df):
    """
    Expands sleep data so each row represents exactly one minute
    """
    # Check if required columns exist and add defaults efficiently
    # If duration is missing, try to infer it or use default
    if 'durationInMs' not in df.columns:
        # Try to find a duration column (optimized: check common names first)
        duration_found = False
        for col in df.columns:
            col_lower = col.lower()
            if any(term in col_lower for term in ['duration', 'length']):
                print(f"Using '{col}' as duration column")
                df['durationInMs'] = df[col]
                duration_found = True
                break
        
        if not duration_found:
            # Use default value
            print("Using default duration of 60000ms (1 minute)")
            df['durationInMs'] = 60000  # Default to 1 minute
    
    # If type is missing, use default
    if 'type' not in df.columns:
        # Try to find a type column (optimized: check common names first)
        type_found = False
        for col in df.columns:
            col_lower = col.lower()
            if any(term in col_lower for term in ['type', 'stage', 'state']):
                print(f"Using '{col}' as type column")
                df['type'] = df[col]
                type_found = True
                break
        
        if not type_found:
            # Use default value
            print("Using default sleep type 'light'")
            df['type'] = 'light'

    # Add sleep summary ID if missing (optimized)
    if 'sleepSummaryId' not in df.columns:
        sleep_id_found = False
        for col in df.columns:
            col_lower = col.lower()
            if any(term in col_lower for term in ['sleep', 'summary', 'id']):
                df['sleepSummaryId'] = df[col]
                sleep_id_found = True
                break
        
        if not sleep_id_found:
            # Create a dummy sleep ID
            df['sleepSummaryId'] = 'sleep_session_1'
    
    # Ensure durationInMs is numeric
    try:
        df['durationInMs'] = pd.to_numeric(df['durationInMs'], errors='coerce').fillna(60000)
        
        # Check if durations are too small (possibly in seconds) - only if we have multiple rows
        # Skip median calculation for single row to improve performance
        if len(df) > 1:
            median_duration = df['durationInMs'].median()
            if median_duration < 1000 and median_duration > 0:
                print("Warning: Durations appear to be in seconds. Converting to milliseconds.")
                df['durationInMs'] = df['durationInMs'] * 1000
    except Exception as e:
        print(f"Error converting duration to numeric: {e}")
        df['durationInMs'] = 60000  # Default to 1 minute
    
    # Ensure we have a timestamp column
    if 'unixTimestampInMs' not in df.columns:
        if 'timestamp' in df.columns:
            try:
                # Try to parse various date formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%m/%d/%Y %H:%M:%S']:
                    try:
                        df['unixTimestampInMs'] = pd.to_datetime(df['timestamp'], format=fmt).astype(int) // 10**6
                        print(f"Successfully parsed timestamps with format: {fmt}")
                        break
                    except:
                        continue
            except:
                pass
        
        # If isoDate exists, use that
        elif 'isoDate' in df.columns:
            try:
                df['unixTimestampInMs'] = pd.to_datetime(df['isoDate']).astype(int) // 10**6
            except:
                pass
        
        # If still not created, use current time
        if 'unixTimestampInMs' not in df.columns:
            # Use Unix timestamp in milliseconds for current time
            current_time = int(datetime.now().timestamp() * 1000)
            df['unixTimestampInMs'] = current_time
    
    # Expand into minute-by-minute records
    expanded_rows = []
    print(f"Expanding {len(df)} sleep records into minute-by-minute data...")
    
    # Convert to dict for faster access - more efficient than iterrows()
    df_dict = df.to_dict('records')
    
    for idx, row in enumerate(df_dict):
        # Skip rows with invalid duration
        duration_ms = row.get('durationInMs', 60000)
        if pd.isna(duration_ms) or duration_ms <= 0:
            continue
        
        # Convert duration to minutes (ceiling to ensure we don't lose any time)
        duration_minutes = max(1, int(np.ceil(duration_ms / 60000)))
        
        # Get base timestamp
        base_timestamp = row.get('unixTimestampInMs', int(datetime.now().timestamp() * 1000))
        
        # Create a row for each minute
        for i in range(duration_minutes):
            # Calculate new timestamp for this minute - properly increment by exactly one minute for each row
            unix_timestamp = base_timestamp + (i * 60000)  # Add i minutes in milliseconds
            
            # Create new record with all original columns (copy dict for efficiency)
            new_record = {k: v for k, v in row.items() if k not in ['durationInMs', 'unixTimestampInMs']}
            
            # Add/override specific columns for minute-by-minute data
            new_record.update({
                'unixTimestampInMs': unix_timestamp,
                'durationInMs': 60000,  # 1 minute in ms
                'TimeInMin': 1.0,  # 1 minute
                'minuteIndex': i + 1,
                'originalDurationMinutes': duration_minutes,
                'originalIndex': idx
            })
            
            # Add readable timestamp that properly increments each minute
            try:
                # Create timestamp that increments with each minute
                dt = datetime.fromtimestamp(unix_timestamp / 1000)
                new_record['Timestamp'] = dt.strftime('%Y-%m-%d %H:%M')
            except Exception as e:
                print(f"Error creating timestamp: {e}")
                # Fall back to base timestamp + minute offset if conversion fails
                if 'isoDate' in row:
                    try:
                        base_dt = datetime.fromisoformat(row['isoDate'].replace('Z', '+00:00'))
                        incremented_dt = base_dt + timedelta(minutes=i)
                        new_record['Timestamp'] = incremented_dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        pass
            
            # Add the participant_id_timestamp column if we have a participant_id
            # This will now have unique timestamps for each minute
            if 'participant_id' in new_record and 'Timestamp' in new_record:
                new_record['participantid_timestamp'] = f"{new_record['participant_id']}_{new_record['Timestamp']}"
            
            expanded_rows.append(new_record)
    
    # Create expanded DataFrame
    if expanded_rows:
        expanded_df = pd.DataFrame(expanded_rows)
        print(f"Expanded to {len(expanded_df)} minute-by-minute records")
        return expanded_df
    else:
        print("Warning: No valid records to expand")
        return df  # Return original dataframe if expansion failed

def process_sleep_data(df, participant_id):
    """
    Processes sleep stage data
    """
    # Add participant ID
    df['participant_id'] = participant_id
    
    # Add timestamp column from isoDate
    if 'isoDate' in df.columns:
        df.insert(
            df.columns.get_loc('isoDate') + 1,
            'Timestamp',
            df['isoDate'].apply(format_iso_date_to_timestamp)
        )
        
        # Add the new column: participantid_timestamp
        df['participantid_timestamp'] = df['participant_id'] + '_' + df['Timestamp'].astype(str)
    
    # Delete specified columns after using them
    columns_to_drop = ['timezoneOffsetInMs', 'isoDate'] 
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
    
    # Add TimeInMin column
    if 'durationInMs' in df.columns:
        df.insert(
            df.columns.get_loc('durationInMs') + 1,
            'TimeInMin',
            df['durationInMs'].apply(convert_ms_to_minutes)
        )
    
    # Expand sleep data to minute-by-minute records
    expanded_df = expand_sleep_data(df)
    
    # Reorder columns to make participant_id first
    cols = ['participant_id'] + [col for col in expanded_df.columns if col != 'participant_id']
    return expanded_df[cols]

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
            print(f"Original dataframe: {df.shape[0]} rows, {df.shape[1]} columns")
            print(f"Original columns: {df.columns.tolist()}")
            
            # Process data
            processed_df = process_sleep_data(df, participant_id)
            print(f"Processed dataframe: {processed_df.shape[0]} rows, {processed_df.shape[1]} columns")
            print(f"Processed columns: {processed_df.columns.tolist()}")

            # Convert to CSV
            output_buffer = StringIO()
            processed_df.to_csv(output_buffer, index=False)

            # Upload to destination
            destination_key = object_key
            s3.put_object(
                Bucket=destination_bucket,
                Key=destination_key,
                Body=output_buffer.getvalue(),
                ContentType='text/csv'
            )

            processed_files.append(object_key)
            print(f"Successfully processed: {object_key} -> {destination_key}")

        except Exception as e:
            print(f"Error processing file: {str(e)}")
            failed_files.append({
                'file': object_key if 'object_key' in locals() else 'unknown',
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
            'message': 'Processed by HealthSleepFunction_DEV with minute-by-minute expansion'
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