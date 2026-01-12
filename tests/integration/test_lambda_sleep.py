"""
Comprehensive tests for the sleep Lambda function.
Includes unit tests with mocked data AND integration tests with real CSV files.
"""
import pytest
import json
import sys
import pandas as pd
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import boto3
import os

# Add the Lambda function path to sys.path
import os
lambda_path = os.path.join(os.path.dirname(__file__), '..', '..', 'services', 'ingestion', 'lambdas', 'albanyHealth-sleep-lambda-function')
lambda_path = os.path.abspath(lambda_path)
if lambda_path not in sys.path:
    sys.path.insert(0, lambda_path)

# Clear any cached main module to avoid conflicts
if 'main' in sys.modules:
    del sys.modules['main']

from main import (
    extract_participantId,
    format_iso_date_to_timestamp,
    convert_ms_to_minutes,
    expand_sleep_data,
    process_sleep_data,
    lambda_handler
)
from tests.conftest import (
    mock_lambda_context,
    sample_lambda_sqs_event,
    set_env_vars
)

# Path to real CSV file for integration tests
# Path to real CSV file for integration tests
REAL_CSV_FILE = Path(__file__).parent.parent / "fixtures" / "real_data" / "251101_garmin-connect-sleep-stage_Testing-1_cd037752.csv"


# ============================================================================
# UNIT TESTS - Testing individual functions with mocked data
# ============================================================================

class TestExtractParticipantId:
    """Test cases for extract_participantId function."""
    
    def test_valid_participant_id(self):
        """Test extracting valid participant ID from object key."""
        object_key = "path/to/file_abc12345.csv"
        result = extract_participantId(object_key)
        assert result == "abc12345"
    
    def test_participant_id_with_numbers(self):
        """Test extracting participant ID with numbers."""
        object_key = "path/to/file_12345678.csv"
        result = extract_participantId(object_key)
        assert result == "12345678"
    
    def test_participant_id_mixed_case(self):
        """Test extracting participant ID - function only supports lowercase."""
        # The regex pattern only matches lowercase [a-z0-9], so mixed case won't match
        object_key = "path/to/file_AbC12345.csv"
        result = extract_participantId(object_key)
        assert result is None  # Mixed case doesn't match the regex pattern
    
    def test_invalid_format_no_match(self):
        """Test extracting participant ID from invalid format."""
        object_key = "path/to/file.csv"
        result = extract_participantId(object_key)
        assert result is None
    
    def test_wrong_length(self):
        """Test extracting participant ID with wrong length."""
        object_key = "path/to/file_abc123.csv"  # Only 6 chars
        result = extract_participantId(object_key)
        assert result is None
    
    def test_empty_string(self):
        """Test extracting participant ID from empty string."""
        object_key = ""
        result = extract_participantId(object_key)
        assert result is None


class TestFormatIsoDateToTimestamp:
    """Test cases for format_iso_date_to_timestamp function."""
    
    def test_valid_iso_with_z(self):
        """Test formatting ISO date with Z timezone."""
        iso_date = "2025-01-01T10:00:00Z"
        result = format_iso_date_to_timestamp(iso_date)
        assert result == "2025-01-01 10:00"
    
    def test_valid_iso_with_timezone(self):
        """Test formatting ISO date with timezone offset."""
        iso_date = "2025-01-01T10:00:00+00:00"
        result = format_iso_date_to_timestamp(iso_date)
        assert result == "2025-01-01 10:00"
    
    def test_valid_iso_with_milliseconds(self):
        """Test formatting ISO date with milliseconds."""
        iso_date = "2025-01-01T10:00:00.000-05:00"
        result = format_iso_date_to_timestamp(iso_date)
        assert result == "2025-01-01 10:00"
    
    def test_invalid_iso_format(self):
        """Test formatting invalid ISO date."""
        iso_date = "invalid-date"
        with pytest.raises(ValueError):
            format_iso_date_to_timestamp(iso_date)


class TestConvertMsToMinutes:
    """Test cases for convert_ms_to_minutes function."""
    
    def test_valid_milliseconds(self):
        """Test converting valid milliseconds to minutes."""
        result = convert_ms_to_minutes(60000)  # 1 minute
        assert result == 1.0
    
    def test_zero_milliseconds(self):
        """Test converting zero milliseconds."""
        result = convert_ms_to_minutes(0)
        assert result == 0.0
    
    def test_large_duration(self):
        """Test converting large duration."""
        result = convert_ms_to_minutes(3600000)  # 1 hour
        assert result == 60.0
    
    def test_fractional_minutes(self):
        """Test converting to fractional minutes."""
        result = convert_ms_to_minutes(90000)  # 1.5 minutes
        assert result == 1.5


class TestExpandSleepData:
    """Test cases for expand_sleep_data function."""
    
    def test_expand_single_record(self):
        """Test expanding a single sleep record to minute-by-minute."""
        df = pd.DataFrame({
            'type': ['light'],
            'durationInMs': [180000],  # 3 minutes
            'unixTimestampInMs': [1704110400000],
            'isoDate': ['2025-01-01T10:00:00Z']
        })
        
        result = expand_sleep_data(df)
        
        assert len(result) == 3  # Should expand to 3 rows (3 minutes)
        assert all(result['durationInMs'] == 60000)  # Each row should be 1 minute
        assert all(result['TimeInMin'] == 1.0)  # Each row should be 1 minute
        assert 'minuteIndex' in result.columns
        assert result['minuteIndex'].tolist() == [1, 2, 3]
    
    def test_expand_multiple_records(self):
        """Test expanding multiple sleep records."""
        df = pd.DataFrame({
            'type': ['light', 'deep'],
            'durationInMs': [120000, 180000],  # 2 minutes, 3 minutes
            'unixTimestampInMs': [1704110400000, 1704110520000],
            'isoDate': ['2025-01-01T10:00:00Z', '2025-01-01T10:02:00Z']
        })
        
        result = expand_sleep_data(df)
        
        assert len(result) == 5  # 2 + 3 = 5 rows
        assert all(result['durationInMs'] == 60000)  # All should be 1 minute
    
    def test_expand_preserves_original_columns(self):
        """Test that original columns are preserved in expanded data."""
        df = pd.DataFrame({
            'type': ['light'],
            'durationInMs': [120000],
            'unixTimestampInMs': [1704110400000],
            'sleepSummaryId': ['sleep_123']
        })
        
        result = expand_sleep_data(df)
        
        assert 'type' in result.columns
        assert 'sleepSummaryId' in result.columns
        assert all(result['type'] == 'light')
        assert all(result['sleepSummaryId'] == 'sleep_123')
    
    def test_expand_creates_timestamp_column(self):
        """Test that Timestamp column is created in expanded data."""
        df = pd.DataFrame({
            'type': ['light'],
            'durationInMs': [120000],
            'unixTimestampInMs': [1704110400000]
        })
        
        result = expand_sleep_data(df)
        
        assert 'Timestamp' in result.columns
        assert len(result['Timestamp'].unique()) == 2  # 2 unique timestamps for 2 minutes
    
    def test_expand_increments_timestamps(self):
        """Test that timestamps increment correctly for each minute."""
        df = pd.DataFrame({
            'type': ['light'],
            'durationInMs': [180000],  # 3 minutes
            'unixTimestampInMs': [1704110400000]  # Base timestamp
        })
        
        result = expand_sleep_data(df)
        
        # Check that timestamps increment by 1 minute (60000 ms) each
        timestamps = result['unixTimestampInMs'].tolist()
        assert timestamps[1] == timestamps[0] + 60000
        assert timestamps[2] == timestamps[1] + 60000
    
    def test_expand_handles_missing_duration(self):
        """Test that missing duration uses default value."""
        df = pd.DataFrame({
            'type': ['light'],
            'unixTimestampInMs': [1704110400000]
        })
        
        result = expand_sleep_data(df)
        
        # Should use default duration of 60000ms (1 minute)
        assert len(result) == 1
        assert all(result['durationInMs'] == 60000)
    
    def test_expand_handles_missing_type(self):
        """Test that missing type uses default value."""
        df = pd.DataFrame({
            'durationInMs': [120000],
            'unixTimestampInMs': [1704110400000]
        })
        
        result = expand_sleep_data(df)
        
        # Should use default type 'light'
        assert 'type' in result.columns
        assert all(result['type'] == 'light')
    
    def test_expand_skips_invalid_duration(self):
        """Test that rows with invalid duration are skipped."""
        df = pd.DataFrame({
            'type': ['light', 'deep'],
            'durationInMs': [120000, 0],  # Second row has invalid duration
            'unixTimestampInMs': [1704110400000, 1704110520000]
        })
        
        result = expand_sleep_data(df)
        
        # Should only expand the first row (2 minutes)
        assert len(result) == 2
        assert all(result['type'] == 'light')
    
    def test_expand_handles_missing_unix_timestamp(self):
        """Test that missing unixTimestampInMs is handled."""
        df = pd.DataFrame({
            'type': ['light'],
            'durationInMs': [120000],
            'isoDate': ['2025-01-01T10:00:00Z']
        })
        
        result = expand_sleep_data(df)
        
        # Should create unixTimestampInMs from isoDate
        assert 'unixTimestampInMs' in result.columns
        assert len(result) == 2


class TestProcessSleepData:
    """Test cases for process_sleep_data function."""
    
    def test_add_participant_id(self):
        """Test that participant_id is added to dataframe."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'type': ['light'],
            'durationInMs': [120000]
        })
        participant_id = "abc12345"
        
        result = process_sleep_data(df, participant_id)
        
        assert 'participant_id' in result.columns
        assert result['participant_id'].iloc[0] == participant_id
    
    def test_create_timestamp_column(self):
        """Test that Timestamp column is created from isoDate."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'type': ['light'],
            'durationInMs': [120000]
        })
        participant_id = "abc12345"
        
        result = process_sleep_data(df, participant_id)
        
        assert 'Timestamp' in result.columns
    
    def test_create_participantid_timestamp(self):
        """Test that participantid_timestamp column is created."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'type': ['light'],
            'durationInMs': [120000]
        })
        participant_id = "abc12345"
        
        result = process_sleep_data(df, participant_id)
        
        assert 'participantid_timestamp' in result.columns
    
    def test_add_time_in_min_column(self):
        """Test that TimeInMin column is added."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'type': ['light'],
            'durationInMs': [120000]  # 2 minutes
        })
        participant_id = "abc12345"
        
        result = process_sleep_data(df, participant_id)
        
        # After expansion, each row should have TimeInMin = 1.0
        assert 'TimeInMin' in result.columns
        assert all(result['TimeInMin'] == 1.0)
    
    def test_drop_specified_columns(self):
        """Test that specified columns are dropped."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'type': ['light'],
            'durationInMs': [120000],
            'timezoneOffsetInMs': [-14400000],
            'unixTimestampInMs': [1704110400000]
        })
        participant_id = "abc12345"
        
        result = process_sleep_data(df, participant_id)
        
        assert 'timezoneOffsetInMs' not in result.columns
        assert 'isoDate' not in result.columns
        assert 'unixTimestampInMs' in result.columns  # Should be kept
    
    def test_expand_to_minute_by_minute(self):
        """Test that data is expanded to minute-by-minute records."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'type': ['light'],
            'durationInMs': [180000]  # 3 minutes
        })
        participant_id = "abc12345"
        
        result = process_sleep_data(df, participant_id)
        
        # Should expand to 3 rows (3 minutes)
        assert len(result) == 3
        assert all(result['durationInMs'] == 60000)  # Each row is 1 minute
    
    def test_reorder_columns(self):
        """Test that participant_id is first column."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'type': ['light'],
            'durationInMs': [120000]
        })
        participant_id = "abc12345"
        
        result = process_sleep_data(df, participant_id)
        
        assert result.columns[0] == 'participant_id'
    
    def test_missing_isodate_column(self):
        """Test handling of missing isoDate column."""
        df = pd.DataFrame({
            'type': ['light'],
            'durationInMs': [120000],
            'unixTimestampInMs': [1704110400000]
        })
        participant_id = "abc12345"
        
        result = process_sleep_data(df, participant_id)
        
        assert 'participant_id' in result.columns
        # Timestamp might not be created without isoDate, but expansion should still work


class TestLambdaHandler:
    """Test cases for lambda_handler function with mocked data."""
    
    @mock_aws
    def test_valid_sqs_message_with_valid_csv(self, mock_lambda_context, set_env_vars):
        """Test processing valid SQS message with valid CSV."""
        # Sample sleep CSV content
        sample_sleep_csv = """Header line 1
Header line 2
Header line 3
Header line 4
Header line 5
Header line 6
isoDate,unixTimestampInMs,type,durationInMs,timezoneOffsetInMs
2025-01-01T10:00:00Z,1704110400000,light,180000,0
2025-01-01T10:03:00Z,1704110580000,deep,120000,0
"""
        
        # Setup S3
        s3 = boto3.client('s3', region_name='us-east-1')
        source_bucket = 'test-source-bucket'
        dest_bucket = 'test-destination-bucket'
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=dest_bucket)
        
        # Upload test file
        object_key = "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-connect-sleep-stage/250204_sleep_Testing-1_cd037752.csv"
        s3.put_object(
            Bucket=source_bucket,
            Key=object_key,
            Body=sample_sleep_csv
        )
        
        # Create event
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key,
                        "folder_name": "garmin-connect-sleep-stage"
                    })
                }
            ]
        }
        
        os.environ['DESTINATION_BUCKET'] = dest_bucket
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['processed_files'] == 1
        assert body['failed_files'] == 0
        
        # Verify file was uploaded to destination
        response = s3.get_object(Bucket=dest_bucket, Key=object_key)
        assert response is not None
        
        # Verify expanded data
        processed_content = response['Body'].read().decode('utf-8')
        processed_df = pd.read_csv(StringIO(processed_content))
        
        # Should have more rows than original (due to expansion)
        assert len(processed_df) > 2  # Original had 2 rows, expanded should have more
    
    @mock_aws
    def test_invalid_participant_id(self, mock_lambda_context, set_env_vars):
        """Test handling of invalid participant ID in file path."""
        sample_sleep_csv = """Header line 1
Header line 2
Header line 3
Header line 4
Header line 5
Header line 6
isoDate,type,durationInMs
2025-01-01T10:00:00Z,light,120000
"""
        
        s3 = boto3.client('s3', region_name='us-east-1')
        source_bucket = 'test-source-bucket'
        dest_bucket = 'test-destination-bucket'
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=dest_bucket)
        
        # Upload file with invalid participant ID format
        object_key = "Health_20Gizmo-08022025/Testing-1_invalid/file.csv"
        s3.put_object(
            Bucket=source_bucket,
            Key=object_key,
            Body=sample_sleep_csv
        )
        
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key,
                        "folder_name": "garmin-connect-sleep-stage"
                    })
                }
            ]
        }
        
        os.environ['DESTINATION_BUCKET'] = dest_bucket
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['failed_files'] == 1
    
    @mock_aws
    def test_s3_file_read_failure(self, mock_lambda_context, set_env_vars):
        """Test handling of S3 file read failure."""
        s3 = boto3.client('s3', region_name='us-east-1')
        source_bucket = 'test-source-bucket'
        dest_bucket = 'test-destination-bucket'
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=dest_bucket)
        
        # Don't upload file - will cause read failure
        object_key = "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-connect-sleep-stage/250204_sleep_Testing-1_cd037752.csv"
        
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key,
                        "folder_name": "garmin-connect-sleep-stage"
                    })
                }
            ]
        }
        
        os.environ['DESTINATION_BUCKET'] = dest_bucket
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['failed_files'] == 1
    
    @mock_aws
    def test_multiple_records_batch(self, mock_lambda_context, set_env_vars):
        """Test processing multiple records in a batch."""
        sample_sleep_csv = """Header line 1
Header line 2
Header line 3
Header line 4
Header line 5
Header line 6
isoDate,type,durationInMs
2025-01-01T10:00:00Z,light,120000
"""
        
        s3 = boto3.client('s3', region_name='us-east-1')
        source_bucket = 'test-source-bucket'
        dest_bucket = 'test-destination-bucket'
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=dest_bucket)
        
        # Upload two files (must have participant ID in filename)
        object_key1 = "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-connect-sleep-stage/file1_cd037752.csv"
        object_key2 = "Health_20Gizmo-08022025/Testing-2_ab123456/garmin-connect-sleep-stage/file2_ab123456.csv"
        
        s3.put_object(Bucket=source_bucket, Key=object_key1, Body=sample_sleep_csv)
        s3.put_object(Bucket=source_bucket, Key=object_key2, Body=sample_sleep_csv)
        
        event = {
            "Records": [
                {
                    "messageId": "msg-1",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key1,
                        "folder_name": "garmin-connect-sleep-stage"
                    })
                },
                {
                    "messageId": "msg-2",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key2,
                        "folder_name": "garmin-connect-sleep-stage"
                    })
                }
            ]
        }
        
        os.environ['DESTINATION_BUCKET'] = dest_bucket
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['processed_files'] == 2


# ============================================================================
# INTEGRATION TESTS - Testing with real CSV files
# ============================================================================

class TestSleepWithRealData:
    """Integration tests using real CSV file."""
    
    def test_real_file_exists(self):
        """Verify the real CSV file exists."""
        if not REAL_CSV_FILE.exists():
            pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
        print(f"\n✓ Found real CSV file: {REAL_CSV_FILE.name}")
    
    def test_read_real_csv_structure(self):
        """Read and display the structure of the real CSV file."""
        if not REAL_CSV_FILE.exists():
            pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
        
        # Read the real CSV file (skip first 6 header rows)
        df = pd.read_csv(REAL_CSV_FILE, skiprows=6, encoding='utf-8')
        
        print(f"\n📄 Real CSV File Structure:")
        print(f"  File: {REAL_CSV_FILE.name}")
        print(f"  Total rows: {len(df)}")
        print(f"  Total columns: {len(df.columns)}")
        print(f"  Columns: {df.columns.tolist()}")
        print(f"\n  First 3 rows of actual data:")
        print(df.head(3).to_string())
        
        # Verify it has data
        assert len(df) > 0, "CSV file is empty!"
        assert len(df.columns) > 0, "CSV file has no columns!"
    
    def test_extract_participant_id_from_real_file(self):
        """Test extracting participant ID from the real file name."""
        if not REAL_CSV_FILE.exists():
            pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
        
        file_name = REAL_CSV_FILE.name
        participant_id = extract_participantId(file_name)
        
        print(f"\n📄 File name: {file_name}")
        print(f"  Extracted participant ID: {participant_id}")
        
        assert participant_id is not None, f"Failed to extract participant ID from {file_name}"
        assert participant_id == "cd037752", f"Expected 'cd037752', got '{participant_id}'"
    
    def test_process_real_csv_data(self):
        """Process the real CSV file and show the transformation."""
        if not REAL_CSV_FILE.exists():
            pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
        
        print(f"\n📄 Processing real CSV file: {REAL_CSV_FILE.name}")
        
        # Read the real CSV file
        df = pd.read_csv(REAL_CSV_FILE, skiprows=6, encoding='utf-8')
        
        print(f"\n  BEFORE Processing:")
        print(f"    Rows: {len(df)}")
        print(f"    Columns: {df.columns.tolist()}")
        
        # Extract participant ID
        participant_id = extract_participantId(REAL_CSV_FILE.name)
        assert participant_id is not None
        
        # Process the real data using your Lambda function logic
        processed_df = process_sleep_data(df, participant_id)
        
        print(f"\n  AFTER Processing (Expanded):")
        print(f"    Rows: {len(processed_df)}")
        print(f"    Columns: {processed_df.columns.tolist()}")
        print(f"    Expansion: {len(df)} original rows → {len(processed_df)} expanded rows")
        
        # Verify processing worked
        assert 'participant_id' in processed_df.columns, "Missing participant_id column"
        assert processed_df['participant_id'].iloc[0] == participant_id, "Participant ID mismatch"
        assert len(processed_df) >= len(df), "Expanded rows should be >= original (minute-by-minute expansion)"
        
        # Check for expected processed columns
        if 'isoDate' in df.columns:
            assert 'participantid_timestamp' in processed_df.columns, "Missing participantid_timestamp column"
        
        print(f"\n  ✓ Successfully processed and expanded {len(processed_df)} rows!")
    
    def test_verify_column_transformations_with_real_data(self):
        """Verify that columns are transformed correctly with real data."""
        if not REAL_CSV_FILE.exists():
            pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
        
        df = pd.read_csv(REAL_CSV_FILE, skiprows=6, encoding='utf-8')
        participant_id = extract_participantId(REAL_CSV_FILE.name)
        processed_df = process_sleep_data(df, participant_id)
        
        print(f"\n📄 Column Transformation Verification:")
        print(f"  Original columns: {df.columns.tolist()}")
        print(f"  Processed columns: {processed_df.columns.tolist()}")
        
        # Verify participant_id is first
        assert processed_df.columns[0] == 'participant_id', "participant_id should be first column"
        
        # Verify dropped columns are gone
        columns_to_drop = ['timezoneOffsetInMs', 'isoDate']
        for col in columns_to_drop:
            if col in df.columns:
                assert col not in processed_df.columns, f"{col} should be dropped"
        
        # Verify kept/added columns
        if 'unixTimestampInMs' in df.columns:
            assert 'unixTimestampInMs' in processed_df.columns, "unixTimestampInMs should be kept"
        assert 'TimeInMin' in processed_df.columns, "TimeInMin should be added"
        assert 'type' in processed_df.columns, "type should be kept"
        
        print(f"  ✓ All column transformations verified!")
    
    def test_verify_expansion_with_real_data(self):
        """Verify that sleep data is expanded to minute-by-minute with real data."""
        if not REAL_CSV_FILE.exists():
            pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
        
        df = pd.read_csv(REAL_CSV_FILE, skiprows=6, encoding='utf-8')
        participant_id = extract_participantId(REAL_CSV_FILE.name)
        processed_df = process_sleep_data(df, participant_id)
        
        print(f"\n📄 Expansion Verification with Real Data:")
        print(f"  Original rows: {len(df)}")
        print(f"  Expanded rows: {len(processed_df)}")
        
        # Verify expansion happened
        assert len(processed_df) >= len(df), "Should have at least as many rows after expansion"
        
        # Verify each row is 1 minute
        assert all(processed_df['durationInMs'] == 60000), "All rows should be 1 minute (60000ms)"
        assert all(processed_df['TimeInMin'] == 1.0), "All rows should be 1 minute"
        
        # Verify minuteIndex exists
        assert 'minuteIndex' in processed_df.columns, "minuteIndex should be added"
        
        print(f"  ✓ Expansion verified - {len(processed_df)} minute-by-minute records created!")
    
    @mock_aws
    def test_lambda_handler_with_real_file(self, mock_lambda_context, set_env_vars):
        """Test the full lambda_handler function with the real CSV file."""
        if not REAL_CSV_FILE.exists():
            pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
        
        print(f"\n📄 Testing Lambda Handler with real file: {REAL_CSV_FILE.name}")
        
        # Read the real CSV file content
        with open(REAL_CSV_FILE, 'r', encoding='utf-8') as f:
            file_content = f.read()
        
        # Setup S3 with mock
        s3 = boto3.client('s3', region_name='us-east-1')
        source_bucket = 'test-source-bucket'
        dest_bucket = 'test-destination-bucket'
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=dest_bucket)
        
        # Upload real file to S3
        object_key = f"Health_20Gizmo-12012026/Testing-1_cd037752/garmin-connect-sleep-stage/{REAL_CSV_FILE.name}"
        s3.put_object(
            Bucket=source_bucket,
            Key=object_key,
            Body=file_content.encode('utf-8'),
            ContentType='text/csv'
        )
        
        # Create Lambda event
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key,
                        "folder_name": "garmin-connect-sleep-stage"
                    })
                }
            ]
        }
        
        os.environ['DESTINATION_BUCKET'] = dest_bucket
        
        # Execute lambda handler
        result = lambda_handler(event, mock_lambda_context)
        
        # Verify success
        assert result['statusCode'] == 200, "Lambda should return 200"
        body = json.loads(result['body'])
        assert body['processed_files'] == 1, "Should process 1 file"
        assert body['failed_files'] == 0, "Should have no failures"
        
        # Verify file was uploaded to destination
        response = s3.get_object(Bucket=dest_bucket, Key=object_key)
        assert response is not None, "Processed file should be in destination bucket"
        
        # Read and verify processed file
        processed_content = response['Body'].read().decode('utf-8')
        processed_df = pd.read_csv(StringIO(processed_content))
        
        assert 'participant_id' in processed_df.columns
        assert len(processed_df) > 0
        
        # Verify expansion happened
        assert len(processed_df) > 0, "Should have expanded rows"
        assert all(processed_df['durationInMs'] == 60000), "All rows should be 1 minute"
        
        print(f"\n  ✓ Lambda handler successfully processed real file!")
        print(f"    Expanded to {len(processed_df)} minute-by-minute records")
    
    def test_real_data_statistics(self):
        """Show statistics about the real data."""
        if not REAL_CSV_FILE.exists():
            pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
        
        df = pd.read_csv(REAL_CSV_FILE, skiprows=6, encoding='utf-8')
        participant_id = extract_participantId(REAL_CSV_FILE.name)
        processed_df = process_sleep_data(df, participant_id)
        
        print(f"\n📊 Real Data Statistics:")
        print(f"  File: {REAL_CSV_FILE.name}")
        print(f"  Participant ID: {participant_id}")
        print(f"  Original records: {len(df)}")
        print(f"  Expanded records: {len(processed_df)}")
        print(f"  Expansion ratio: {len(processed_df) / len(df):.2f}x")
        
        # Show statistics for sleep types
        if 'type' in processed_df.columns:
            print(f"\n  Sleep Type Distribution:")
            type_counts = processed_df['type'].value_counts()
            for sleep_type, count in type_counts.items():
                print(f"    {sleep_type}: {count} minutes ({count/len(processed_df)*100:.1f}%)")
        
        if 'unixTimestampInMs' in processed_df.columns:
            print(f"\n  Timestamp Range:")
            print(f"    First: {pd.to_datetime(processed_df['unixTimestampInMs'].min(), unit='ms')}")
            print(f"    Last: {pd.to_datetime(processed_df['unixTimestampInMs'].max(), unit='ms')}")
        
        print(f"\n  ✓ Statistics calculated successfully!")
