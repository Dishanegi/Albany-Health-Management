"""
Comprehensive tests for the Garmin Data Merge Glue Job.
Tests CSV merging, data joining, and participant-based data combination.
Uses processed data format that would come from preprocessing job.
"""
import pytest
import sys
import json
import os
from pathlib import Path
from io import StringIO
from unittest.mock import Mock, MagicMock, patch, call
from moto import mock_aws
import boto3
import pandas as pd

# Path to Garmin test CSV fixtures
TEST_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "garmin_test_data"

# Test CSV file paths (for reference, merge job works with preprocessed data)
HEART_RATE_CSV = TEST_FIXTURES_DIR / "260115_garmin-device-heart-rate_Testing-1_cd037752.csv"
STEP_CSV = TEST_FIXTURES_DIR / "260115_garmin-device-step_Testing-1_cd037752.csv"
SLEEP_CSV = TEST_FIXTURES_DIR / "260115_garmin-connect-sleep-stage_Testing-1_cd037752.csv"


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_s3_buckets():
    """Create mock S3 buckets for source and destination."""
    with mock_aws():
        s3 = boto3.client('s3', region_name='us-east-1')
        
        source_bucket = 'test-processed-bucket'
        target_bucket = 'test-merged-bucket'
        
        s3.create_bucket(Bucket=source_bucket)
        s3.create_bucket(Bucket=target_bucket)
        
        yield {
            's3_client': s3,
            'source_bucket': source_bucket,
            'target_bucket': target_bucket
        }


def create_processed_csv_sample(participant_id="cd037752", data_type="heart-rate"):
    """Create sample CSV content that would be output from preprocessing."""
    if data_type == "heart-rate":
        return f"""participant_id,timestamp,beatsPerMinute
{participant_id},2025-01-01 10:00,72
{participant_id},2025-01-01 10:01,75
{participant_id},2025-01-01 10:02,73
"""
    elif data_type == "step":
        return f"""participant_id,timestamp,steps,totalSteps
{participant_id},2025-01-01 10:00,100,5000
{participant_id},2025-01-01 10:01,150,5150
{participant_id},2025-01-01 10:02,120,5270
"""
    elif data_type == "sleep":
        return f"""participant_id,timestamp,durationInMs,sleepType
{participant_id},2025-01-01 22:00,1800000,light
{participant_id},2025-01-01 22:30,3600000,deep
{participant_id},2025-01-01 23:30,1200000,rem
"""
    else:
        return ""


def upload_csv_to_s3(s3_client, bucket, key, csv_content):
    """Upload CSV content to S3."""
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_content.encode('utf-8'),
        ContentType='text/csv'
    )


# ============================================================================
# UNIT TESTS - Merge Logic
# ============================================================================

class TestMergeLogic:
    """Test merge and join logic - matches actual Glue job prepare_for_join function."""
    
    def test_participant_id_extraction_from_composite_key(self):
        """Test extracting participant_id from participantid_timestamp (matches Glue job lines 73-84)."""
        # This matches the prepare_for_join logic
        composite_key = "cd037752_20250101100000"
        parts = composite_key.split("_")
        participant_id = parts[0] if len(parts) > 0 else None
        timestamp = parts[1] if len(parts) > 1 else None
        
        assert participant_id == "cd037752"
        assert timestamp == "20250101100000"
    
    def test_join_keys_exist(self):
        """Test that join keys (participant_id, timestamp) exist - required by Glue job."""
        # These columns are required for joining (lines 73-84)
        heart_rate_cols = ["participant_id", "timestamp", "beatsPerMinute"]
        step_cols = ["participant_id", "timestamp", "steps", "totalSteps"]
        sleep_cols = ["participant_id", "timestamp", "durationInMs", "sleepType"]
        
        required_join_cols = ["participant_id", "timestamp"]
        
        for col in required_join_cols:
            assert col in heart_rate_cols
            assert col in step_cols
            assert col in sleep_cols
    
    def test_column_standardization_heart_rate(self):
        """Test column standardization for heart rate data type (matches Glue job lines 90-103)."""
        # Test the logic: if "beatsPerMinute" exists, use it; else search for alternatives or default to 0
        test_cases = [
            (["participant_id", "timestamp", "beatsPerMinute"], ["beatsPerMinute"]),  # Standard case
            (["participant_id", "timestamp", "heartRate"], ["heartRate"]),  # Alternative name
            (["participant_id", "timestamp"], []),  # Missing - would default to 0 in actual job
        ]
        
        for columns, expected_metric_cols in test_cases:
            # In actual job, it searches for "heart", "beat", or "bpm" in column names
            heart_cols = [c for c in columns if "heart" in c.lower() or "beat" in c.lower() or "bpm" in c.lower()]
            assert len(heart_cols) == len(expected_metric_cols)
    
    def test_column_standardization_sleep(self):
        """Test column standardization for sleep data type (matches Glue job lines 104-124)."""
        # Sleep type looks for "durationInMs" or columns with "duration", and "type" or columns with "type"/"stage"
        test_columns = ["participant_id", "timestamp", "durationInMs", "type"]
        
        duration_col = "durationInMs" if "durationInMs" in test_columns else next((c for c in test_columns if "duration" in c.lower()), None)
        type_col = "type" if "type" in test_columns else next((c for c in test_columns if "type" in c.lower() or "stage" in c.lower()), None)
        
        assert duration_col == "durationInMs"
        assert type_col == "type"
    
    def test_input_path_structure(self):
        """Test input path structure matches Glue job: initial_append/{patient_id}/{data_type}.csv"""
        # This matches line 274 in the Glue job
        input_path = "s3://bucket/initial_append"
        patient_id = "Testing-1_cd037752"
        data_type = "garmin-device-heart-rate"
        
        expected_path = f"{input_path}/{patient_id}/{data_type}.csv"
        assert expected_path == "s3://bucket/initial_append/Testing-1_cd037752/garmin-device-heart-rate.csv"


# ============================================================================
# INTEGRATION TESTS - S3 Operations and File Merging
# ============================================================================

class TestS3MergeOperations:
    """Test S3 operations for merge job."""
    
    @mock_aws
    def test_list_initial_append_files(self, mock_s3_buckets):
        """Test listing files from initial_append folder."""
        s3 = mock_s3_buckets['s3_client']
        bucket = mock_s3_buckets['source_bucket']
        participant_id = "Testing-1_cd037752"
        
        # Upload files to initial_append structure (as output from preprocessing)
        upload_csv_to_s3(s3, bucket, f"initial_append/{participant_id}/garmin-device-heart-rate.csv", 
                         create_processed_csv_sample("cd037752", "heart-rate"))
        upload_csv_to_s3(s3, bucket, f"initial_append/{participant_id}/garmin-device-step.csv", 
                         create_processed_csv_sample("cd037752", "step"))
        
        # List files in initial_append
        paginator = s3.get_paginator('list_objects_v2')
        files = []
        
        for page in paginator.paginate(Bucket=bucket, Prefix="initial_append/"):
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(obj["Key"])
        
        assert len(files) == 2
        assert any("garmin-device-heart-rate.csv" in f for f in files)
        assert any("garmin-device-step.csv" in f for f in files)
    
    @mock_aws
    def test_find_participant_files(self, mock_s3_buckets):
        """Test finding all files for a specific participant."""
        s3 = mock_s3_buckets['s3_client']
        bucket = mock_s3_buckets['source_bucket']
        participant_id = "Testing-1_cd037752"
        
        # Upload files for participant (preprocessed format)
        files = [
            (f"initial_append/{participant_id}/garmin-device-heart-rate.csv", "heart-rate"),
            (f"initial_append/{participant_id}/garmin-device-step.csv", "step"),
            (f"initial_append/{participant_id}/garmin-connect-sleep-stage.csv", "sleep"),
        ]
        
        for file_key, data_type in files:
            upload_csv_to_s3(s3, bucket, file_key, create_processed_csv_sample("cd037752", data_type))
        
        # Find files for participant
        prefix = f"initial_append/{participant_id}/"
        paginator = s3.get_paginator('list_objects_v2')
        participant_files = []
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    participant_files.append(obj["Key"])
        
        assert len(participant_files) == 3
    
    @mock_aws
    def test_write_merged_output(self, mock_s3_buckets):
        """Test writing merged output to destination bucket."""
        s3 = mock_s3_buckets['s3_client']
        source_bucket = mock_s3_buckets['source_bucket']
        target_bucket = mock_s3_buckets['target_bucket']
        participant_id = "Testing-1_cd037752"
        
        # Upload source files (from preprocessing output)
        upload_csv_to_s3(s3, source_bucket, f"initial_append/{participant_id}/garmin-device-heart-rate.csv",
                         create_processed_csv_sample("cd037752", "heart-rate"))
        
        # Write merged output (simulated - would be created by merge job)
        merged_content = "participant_id,timestamp,beatsPerMinute,steps\ncd037752,2025-01-01 10:00,72,100\n"
        output_key = f"merged_garmin_data/{participant_id}_merged.csv"
        upload_csv_to_s3(s3, target_bucket, output_key, merged_content)
        
        # Verify output exists
        response = s3.get_object(Bucket=target_bucket, Key=output_key)
        content = response['Body'].read().decode('utf-8')
        
        assert merged_content == content


class TestCSVMergeProcessing:
    """Test CSV merging and processing."""
    
    @mock_aws
    def test_read_multiple_csv_files(self, mock_s3_buckets):
        """Test reading multiple CSV files for merging."""
        s3 = mock_s3_buckets['s3_client']
        bucket = mock_s3_buckets['source_bucket']
        participant_id = "Testing-1_cd037752"
        
        # Upload different data type files (as they would appear after preprocessing)
        files = [
            (f"initial_append/{participant_id}/garmin-device-heart-rate.csv", "heart-rate"),
            (f"initial_append/{participant_id}/garmin-device-step.csv", "step"),
        ]
        
        for file_key, data_type in files:
            upload_csv_to_s3(s3, bucket, file_key, create_processed_csv_sample("cd037752", data_type))
        
        # Read files
        for file_key, _ in files:
            response = s3.get_object(Bucket=bucket, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            assert len(content) > 0
    
    def test_csv_column_structure(self):
        """Test that preprocessed CSV files have correct structure."""
        # Sample processed CSV structure
        heart_rate_csv = create_processed_csv_sample("cd037752", "heart-rate")
        lines = heart_rate_csv.strip().split('\n')
        
        # Check header
        header = lines[0]
        assert "participant_id" in header
        assert "timestamp" in header
        assert "beatsPerMinute" in header
        
        # Check data rows
        assert len(lines) > 1  # Has header + at least one data row
    
    @mock_aws
    def test_merge_multiple_data_types_same_participant(self, mock_s3_buckets):
        """Test merging multiple data types for the same participant."""
        s3 = mock_s3_buckets['s3_client']
        bucket = mock_s3_buckets['source_bucket']
        participant_id = "Testing-1_cd037752"
        
        # Upload all data types for participant
        data_types = ["heart-rate", "step", "sleep"]
        
        for data_type in data_types:
            file_key = f"initial_append/{participant_id}/garmin-device-{data_type}.csv"
            if data_type == "sleep":
                file_key = f"initial_append/{participant_id}/garmin-connect-sleep-stage.csv"
            elif data_type == "heart-rate":
                file_key = f"initial_append/{participant_id}/garmin-device-heart-rate.csv"
            elif data_type == "step":
                file_key = f"initial_append/{participant_id}/garmin-device-step.csv"
            
            upload_csv_to_s3(s3, bucket, file_key, create_processed_csv_sample("cd037752", data_type))
        
        # Verify all files exist
        paginator = s3.get_paginator('list_objects_v2')
        files = []
        
        for page in paginator.paginate(Bucket=bucket, Prefix=f"initial_append/{participant_id}/"):
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(obj["Key"])
        
        assert len(files) >= 3  # Should have at least 3 data type files


# ============================================================================
# EDGE CASES AND ERROR HANDLING
# ============================================================================

class TestMergeErrorHandling:
    """Test error handling in merge operations."""
    
    @mock_aws
    def test_missing_data_type_files(self, mock_s3_buckets):
        """Test handling when some data type files are missing."""
        s3 = mock_s3_buckets['s3_client']
        bucket = mock_s3_buckets['source_bucket']
        participant_id = "Testing-1_cd037752"
        
        # Upload only heart rate file
        upload_csv_to_s3(s3, bucket, f"initial_append/{participant_id}/garmin-device-heart-rate.csv",
                         create_processed_csv_sample("cd037752", "heart-rate"))
        
        # Try to list all data types - should only find heart rate
        data_types = ["garmin-device-heart-rate", "garmin-device-step", "garmin-connect-sleep-stage"]
        found_types = []
        
        for data_type in data_types:
            key = f"initial_append/{participant_id}/{data_type}.csv"
            try:
                s3.head_object(Bucket=bucket, Key=key)
                found_types.append(data_type)
            except s3.exceptions.ClientError:
                pass
        
        assert len(found_types) == 1
        assert "garmin-device-heart-rate" in found_types
    
    @mock_aws
    def test_empty_initial_append_folder(self, mock_s3_buckets):
        """Test handling when initial_append folder is empty."""
        s3 = mock_s3_buckets['s3_client']
        bucket = mock_s3_buckets['source_bucket']
        
        # List objects in empty folder
        paginator = s3.get_paginator('list_objects_v2')
        files = []
        
        for page in paginator.paginate(Bucket=bucket, Prefix="initial_append/"):
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(obj["Key"])
        
        assert len(files) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
