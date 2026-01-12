"""
Comprehensive tests for the heart rate Lambda function.
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
lambda_path = os.path.join(os.path.dirname(__file__), '..', '..', 'services', 'ingestion', 'lambdas', 'albanyHealth-heart-rate-lambda-function')
lambda_path = os.path.abspath(lambda_path)
if lambda_path not in sys.path:
    sys.path.insert(0, lambda_path)

# Clear any cached main module to avoid conflicts
if 'main' in sys.modules:
    del sys.modules['main']

from main import (
    extract_participant_id,
    format_iso_timestamp,
    process_heart_rate_data,
    lambda_handler
)
from tests.conftest import (
    mock_lambda_context,
    sample_lambda_sqs_event,
    sample_heart_rate_csv,
    set_env_vars
)

# Path to real CSV file for integration tests
REAL_CSV_FILE = Path(__file__).parent.parent / "fixtures" / "real_data" / "251119_garmin-device-heart-rate_Testing-1_cd037752.csv"


# ============================================================================
# UNIT TESTS - Testing individual functions with mocked data
# ============================================================================

class TestExtractParticipantId:
    """Test cases for extract_participant_id function."""
    
    def test_valid_participant_id(self):
        """Test extracting valid participant ID from object key."""
        object_key = "path/to/file_abc12345.csv"
        result = extract_participant_id(object_key)
        assert result == "abc12345"
    
    def test_participant_id_with_numbers(self):
        """Test extracting participant ID with numbers."""
        object_key = "path/to/file_12345678.csv"
        result = extract_participant_id(object_key)
        assert result == "12345678"
    
    def test_participant_id_mixed_case(self):
        """Test extracting participant ID - function only supports lowercase."""
        # The regex pattern only matches lowercase [a-z0-9], so mixed case won't match
        object_key = "path/to/file_AbC12345.csv"
        result = extract_participant_id(object_key)
        assert result is None  # Mixed case doesn't match the regex pattern
    
    def test_invalid_format_no_match(self):
        """Test extracting participant ID from invalid format."""
        object_key = "path/to/file.csv"
        result = extract_participant_id(object_key)
        assert result is None
    
    def test_wrong_length(self):
        """Test extracting participant ID with wrong length."""
        object_key = "path/to/file_abc123.csv"  # Only 6 chars
        result = extract_participant_id(object_key)
        assert result is None
    
    def test_empty_string(self):
        """Test extracting participant ID from empty string."""
        object_key = ""
        result = extract_participant_id(object_key)
        assert result is None


class TestFormatIsoTimestamp:
    """Test cases for format_iso_timestamp function."""
    
    def test_valid_iso_with_z(self):
        """Test formatting ISO date with Z timezone."""
        iso_date = "2025-01-01T10:00:00Z"
        result = format_iso_timestamp(iso_date)
        assert result == "2025-01-01 10:00"
    
    def test_valid_iso_with_timezone(self):
        """Test formatting ISO date with timezone offset."""
        iso_date = "2025-01-01T10:00:00+00:00"
        result = format_iso_timestamp(iso_date)
        assert result == "2025-01-01 10:00"
    
    def test_invalid_iso_format(self):
        """Test formatting invalid ISO date."""
        iso_date = "invalid-date"
        result = format_iso_timestamp(iso_date)
        assert result is None
    
    def test_none_input(self):
        """Test formatting None input."""
        result = format_iso_timestamp(None)
        assert result is None
    
    def test_empty_string(self):
        """Test formatting empty string."""
        result = format_iso_timestamp("")
        assert result is None


class TestProcessHeartRateData:
    """Test cases for process_heart_rate_data function."""
    
    def test_add_participant_id(self):
        """Test that participant_id is added to dataframe."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'heartRate': [72]
        })
        participant_id = "abc12345"
        
        result = process_heart_rate_data(df, participant_id)
        
        assert 'participant_id' in result.columns
        assert result['participant_id'].iloc[0] == participant_id
    
    def test_create_timestamp_column(self):
        """Test that Timestamp column is created from isoDate."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'heartRate': [72]
        })
        participant_id = "abc12345"
        
        result = process_heart_rate_data(df, participant_id)
        
        assert 'Timestamp' in result.columns
        assert result['Timestamp'].iloc[0] == "2025-01-01 10:00"
    
    def test_create_participantid_timestamp(self):
        """Test that participantid_timestamp column is created."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'heartRate': [72]
        })
        participant_id = "abc12345"
        
        result = process_heart_rate_data(df, participant_id)
        
        assert 'participantid_timestamp' in result.columns
        assert result['participantid_timestamp'].iloc[0] == "abc12345_2025-01-01 10:00"
    
    def test_drop_specified_columns(self):
        """Test that specified columns are dropped."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'heartRate': [72],
            'status': ['VALID'],
            'deviceType': ['DEVICE'],
            'timezone': ['UTC'],
            'unixTimestampInMs': [1704110400000]
        })
        participant_id = "abc12345"
        
        result = process_heart_rate_data(df, participant_id)
        
        assert 'status' not in result.columns
        assert 'deviceType' not in result.columns
        assert 'timezone' not in result.columns
        assert 'isoDate' not in result.columns
        assert 'unixTimestampInMs' in result.columns  # Should be kept
    
    def test_reorder_columns(self):
        """Test that participant_id is first column."""
        df = pd.DataFrame({
            'isoDate': ['2025-01-01T10:00:00Z'],
            'heartRate': [72]
        })
        participant_id = "abc12345"
        
        result = process_heart_rate_data(df, participant_id)
        
        assert result.columns[0] == 'participant_id'
    
    def test_missing_isodate_column(self):
        """Test handling of missing isoDate column."""
        df = pd.DataFrame({
            'heartRate': [72],
            'unixTimestampInMs': [1704110400000]
        })
        participant_id = "abc12345"
        
        result = process_heart_rate_data(df, participant_id)
        
        assert 'participant_id' in result.columns
        assert 'Timestamp' not in result.columns  # Should not be created
        assert 'participantid_timestamp' not in result.columns
    
    def test_empty_dataframe(self):
        """Test handling of empty dataframe."""
        df = pd.DataFrame()
        participant_id = "abc12345"
        
        result = process_heart_rate_data(df, participant_id)
        
        assert 'participant_id' in result.columns
        assert len(result) == 0


class TestLambdaHandler:
    """Test cases for lambda_handler function with mocked data."""
    
    @mock_aws
    def test_valid_sqs_message_with_valid_csv(self, mock_lambda_context, set_env_vars, sample_heart_rate_csv):
        """Test processing valid SQS message with valid CSV."""
        # Setup S3
        s3 = boto3.client('s3', region_name='us-east-1')
        source_bucket = 'test-source-bucket'
        dest_bucket = 'test-destination-bucket'
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=dest_bucket)
        
        # Upload test file
        object_key = "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/250204_garmin-device-heart-rate_Testing-1_cd037752.csv"
        s3.put_object(
            Bucket=source_bucket,
            Key=object_key,
            Body=sample_heart_rate_csv
        )
        
        # Create event
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key,
                        "folder_name": "garmin-device-heart-rate"
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
    
    @mock_aws
    def test_invalid_participant_id(self, mock_lambda_context, set_env_vars, sample_heart_rate_csv):
        """Test handling of invalid participant ID in file path."""
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
            Body=sample_heart_rate_csv
        )
        
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key,
                        "folder_name": "garmin-device-heart-rate"
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
        object_key = "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/250204_garmin-device-heart-rate_Testing-1_cd037752.csv"
        
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key,
                        "folder_name": "garmin-device-heart-rate"
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
    def test_invalid_csv_format(self, mock_lambda_context, set_env_vars):
        """Test handling of invalid CSV format."""
        s3 = boto3.client('s3', region_name='us-east-1')
        source_bucket = 'test-source-bucket'
        dest_bucket = 'test-destination-bucket'
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=dest_bucket)
        
        # Upload invalid CSV
        object_key = "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/250204_garmin-device-heart-rate_Testing-1_cd037752.csv"
        s3.put_object(
            Bucket=source_bucket,
            Key=object_key,
            Body="invalid,csv,content\nbroken,data"
        )
        
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key,
                        "folder_name": "garmin-device-heart-rate"
                    })
                }
            ]
        }
        
        os.environ['DESTINATION_BUCKET'] = dest_bucket
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        # May succeed or fail depending on pandas parsing, but should handle gracefully
        assert 'processed_files' in body or 'failed_files' in body
    
    @mock_aws
    def test_multiple_records_batch(self, mock_lambda_context, set_env_vars, sample_heart_rate_csv):
        """Test processing multiple records in a batch."""
        s3 = boto3.client('s3', region_name='us-east-1')
        source_bucket = 'test-source-bucket'
        dest_bucket = 'test-destination-bucket'
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=dest_bucket)
        
        # Upload two files (must have participant ID in filename)
        object_key1 = "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/file1_cd037752.csv"
        object_key2 = "Health_20Gizmo-08022025/Testing-2_ab123456/garmin-device-heart-rate/file2_ab123456.csv"
        
        s3.put_object(Bucket=source_bucket, Key=object_key1, Body=sample_heart_rate_csv)
        s3.put_object(Bucket=source_bucket, Key=object_key2, Body=sample_heart_rate_csv)
        
        event = {
            "Records": [
                {
                    "messageId": "msg-1",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key1,
                        "folder_name": "garmin-device-heart-rate"
                    })
                },
                {
                    "messageId": "msg-2",
                    "body": json.dumps({
                        "source_bucket": source_bucket,
                        "object_key": object_key2,
                        "folder_name": "garmin-device-heart-rate"
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

class TestHeartRateWithRealData:
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
        participant_id = extract_participant_id(file_name)
        
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
        participant_id = extract_participant_id(REAL_CSV_FILE.name)
        assert participant_id is not None
        
        # Process the real data using your Lambda function logic
        processed_df = process_heart_rate_data(df, participant_id)
        
        print(f"\n  AFTER Processing:")
        print(f"    Rows: {len(processed_df)}")
        print(f"    Columns: {processed_df.columns.tolist()}")
        
        # Verify processing worked
        assert 'participant_id' in processed_df.columns, "Missing participant_id column"
        assert processed_df['participant_id'].iloc[0] == participant_id, "Participant ID mismatch"
        assert len(processed_df) == len(df), "Should have same number of rows after processing"
        
        # Check for expected processed columns
        if 'isoDate' in df.columns:
            assert 'participantid_timestamp' in processed_df.columns, "Missing participantid_timestamp column"
        
        print(f"\n  ✓ Successfully processed {len(processed_df)} rows!")
    
    def test_verify_column_transformations_with_real_data(self):
        """Verify that columns are transformed correctly with real data."""
        if not REAL_CSV_FILE.exists():
            pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
        
        df = pd.read_csv(REAL_CSV_FILE, skiprows=6, encoding='utf-8')
        participant_id = extract_participant_id(REAL_CSV_FILE.name)
        processed_df = process_heart_rate_data(df, participant_id)
        
        print(f"\n📄 Column Transformation Verification:")
        print(f"  Original columns: {df.columns.tolist()}")
        print(f"  Processed columns: {processed_df.columns.tolist()}")
        
        # Verify participant_id is first
        assert processed_df.columns[0] == 'participant_id', "participant_id should be first column"
        
        # Verify dropped columns are gone
        columns_to_drop = ['status', 'deviceType', 'timezone', 'isoDate']
        for col in columns_to_drop:
            if col in df.columns:
                assert col not in processed_df.columns, f"{col} should be dropped"
        
        # Verify kept columns
        if 'unixTimestampInMs' in df.columns:
            assert 'unixTimestampInMs' in processed_df.columns, "unixTimestampInMs should be kept"
        
        print(f"  ✓ All column transformations verified!")
    
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
        object_key = f"Health_20Gizmo-20251911/Testing-1_cd037752/garmin-device-heart-rate/{REAL_CSV_FILE.name}"
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
                        "folder_name": "garmin-device-heart-rate"
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
        
        print(f"\n  ✓ Lambda handler successfully processed real file!")
    
    def test_real_data_statistics(self):
        """Show statistics about the real data."""
        if not REAL_CSV_FILE.exists():
            pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
        
        df = pd.read_csv(REAL_CSV_FILE, skiprows=6, encoding='utf-8')
        participant_id = extract_participant_id(REAL_CSV_FILE.name)
        processed_df = process_heart_rate_data(df, participant_id)
        
        print(f"\n📊 Real Data Statistics:")
        print(f"  File: {REAL_CSV_FILE.name}")
        print(f"  Participant ID: {participant_id}")
        print(f"  Total records: {len(processed_df)}")
        
        # Show statistics for numeric columns
        if 'beatsPerMinute' in processed_df.columns:
            print(f"\n  Heart Rate Statistics:")
            print(f"    Min: {processed_df['beatsPerMinute'].min()}")
            print(f"    Max: {processed_df['beatsPerMinute'].max()}")
            print(f"    Mean: {processed_df['beatsPerMinute'].mean():.2f}")
            print(f"    Median: {processed_df['beatsPerMinute'].median():.2f}")
        
        if 'unixTimestampInMs' in processed_df.columns:
            print(f"\n  Timestamp Range:")
            print(f"    First: {pd.to_datetime(processed_df['unixTimestampInMs'].min(), unit='ms')}")
            print(f"    Last: {pd.to_datetime(processed_df['unixTimestampInMs'].max(), unit='ms')}")
        
        print(f"\n  ✓ Statistics calculated successfully!")
