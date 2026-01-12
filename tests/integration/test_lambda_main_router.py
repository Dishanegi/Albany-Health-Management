"""
Integration tests for the main router Lambda function.
Tests the routing logic that directs S3 events to appropriate SQS queues.
"""
import pytest
import json
import sys
from unittest.mock import Mock, patch, MagicMock
from moto import mock_aws
import boto3

# Add the Lambda function path to sys.path
import os
lambda_path = os.path.join(os.path.dirname(__file__), '..', '..', 'services', 'ingestion', 'lambdas', 'albanyHealth-main-router-lambda-function')
lambda_path = os.path.abspath(lambda_path)
if lambda_path not in sys.path:
    sys.path.insert(0, lambda_path)

# Clear any cached main module to avoid conflicts
if 'main' in sys.modules:
    del sys.modules['main']

from main import extract_folder_name, get_queue_url_for_folder, lambda_handler
from tests.conftest import mock_lambda_context, sample_sqs_event, set_env_vars


class TestExtractFolderName:
    """Test cases for extract_folder_name function."""
    
    def test_valid_path_three_segments(self):
        """Test extracting folder name from valid path with 3 segments."""
        object_key = "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/file.csv"
        result = extract_folder_name(object_key)
        assert result == "garmin-device-heart-rate"
    
    def test_valid_path_more_than_three_segments(self):
        """Test extracting folder name from path with more than 3 segments."""
        object_key = "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/subfolder/file.csv"
        result = extract_folder_name(object_key)
        assert result == "garmin-device-heart-rate"
    
    def test_path_with_exactly_three_segments(self):
        """Test extracting folder name from path with exactly 3 segments."""
        object_key = "folder1/folder2/folder3/file.csv"
        result = extract_folder_name(object_key)
        assert result == "folder3"
    
    def test_path_with_less_than_three_segments(self):
        """Test extracting folder name from path with less than 3 segments."""
        object_key = "folder1/file.csv"
        result = extract_folder_name(object_key)
        assert result == ""
    
    def test_path_with_two_segments(self):
        """Test extracting folder name from path with 2 segments."""
        object_key = "folder1/folder2"
        result = extract_folder_name(object_key)
        assert result == ""
    
    def test_empty_string(self):
        """Test extracting folder name from empty string."""
        object_key = ""
        result = extract_folder_name(object_key)
        assert result == ""
    
    def test_single_segment(self):
        """Test extracting folder name from single segment path."""
        object_key = "file.csv"
        result = extract_folder_name(object_key)
        assert result == ""


class TestGetQueueUrlForFolder:
    """Test cases for get_queue_url_for_folder function."""
    
    def test_heart_rate_folder(self, set_env_vars):
        """Test that heart rate folder maps to correct queue."""
        result = get_queue_url_for_folder('garmin-device-heart-rate')
        assert result == set_env_vars['HEALTH_HEART_RATE_QUEUE']
    
    def test_sleep_folder(self, set_env_vars):
        """Test that sleep folder maps to correct queue."""
        result = get_queue_url_for_folder('garmin-connect-sleep-stage')
        assert result == set_env_vars['HEALTH_SLEEP_QUEUE']
    
    def test_step_folder(self, set_env_vars):
        """Test that step folder maps to correct queue."""
        result = get_queue_url_for_folder('garmin-device-step')
        assert result == set_env_vars['HEALTH_STEP_QUEUE']
    
    def test_pulse_ox_folder(self, set_env_vars):
        """Test that pulse ox folder maps to others queue."""
        result = get_queue_url_for_folder('garmin-device-pulse-ox')
        assert result == set_env_vars['HEALTH_OTHERS_QUEUE']
    
    def test_respiration_folder(self, set_env_vars):
        """Test that respiration folder maps to others queue."""
        result = get_queue_url_for_folder('garmin-device-respiration')
        assert result == set_env_vars['HEALTH_OTHERS_QUEUE']
    
    def test_stress_folder(self, set_env_vars):
        """Test that stress folder maps to others queue."""
        result = get_queue_url_for_folder('garmin-device-stress')
        assert result == set_env_vars['HEALTH_OTHERS_QUEUE']
    
    def test_unknown_folder(self, set_env_vars):
        """Test that unknown folder returns None."""
        result = get_queue_url_for_folder('unknown-folder')
        assert result is None
    
    def test_missing_environment_variables(self, monkeypatch):
        """Test behavior when environment variables are missing."""
        # Remove environment variables
        monkeypatch.delenv('HEALTH_HEART_RATE_QUEUE', raising=False)
        monkeypatch.delenv('HEALTH_SLEEP_QUEUE', raising=False)
        monkeypatch.delenv('HEALTH_STEP_QUEUE', raising=False)
        monkeypatch.delenv('HEALTH_OTHERS_QUEUE', raising=False)
        
        result = get_queue_url_for_folder('garmin-device-heart-rate')
        assert result is None


class TestLambdaHandler:
    """Test cases for lambda_handler function."""
    
    @mock_aws
    def test_single_valid_record(self, mock_lambda_context, set_env_vars):
        """Test processing a single valid SQS record."""
        # Create SQS queue
        sqs = boto3.client('sqs', region_name='us-east-1')
        queue_url = sqs.create_queue(QueueName='test-heart-rate-queue')['QueueUrl']
        
        # Update environment variable to use the mock queue
        import os
        os.environ['HEALTH_HEART_RATE_QUEUE'] = queue_url
        
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": "test-bucket"},
                                    "object": {
                                        "key": "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/file.csv"
                                    }
                                }
                            }
                        ]
                    })
                }
            ]
        }
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['processed_messages'] == 1
        assert body['failed_messages'] == 0
    
    @mock_aws
    def test_multiple_records(self, mock_lambda_context, set_env_vars):
        """Test processing multiple SQS records."""
        sqs = boto3.client('sqs', region_name='us-east-1')
        heart_rate_queue = sqs.create_queue(QueueName='test-heart-rate-queue')['QueueUrl']
        sleep_queue = sqs.create_queue(QueueName='test-sleep-queue')['QueueUrl']
        
        import os
        os.environ['HEALTH_HEART_RATE_QUEUE'] = heart_rate_queue
        os.environ['HEALTH_SLEEP_QUEUE'] = sleep_queue
        
        event = {
            "Records": [
                {
                    "messageId": "msg-1",
                    "body": json.dumps({
                        "Records": [{
                            "s3": {
                                "bucket": {"name": "test-bucket"},
                                "object": {
                                    "key": "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/file1.csv"
                                }
                            }
                        }]
                    })
                },
                {
                    "messageId": "msg-2",
                    "body": json.dumps({
                        "Records": [{
                            "s3": {
                                "bucket": {"name": "test-bucket"},
                                "object": {
                                    "key": "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-connect-sleep-stage/file2.csv"
                                }
                            }
                        }]
                    })
                }
            ]
        }
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['processed_messages'] == 2
    
    def test_invalid_json_in_body(self, mock_lambda_context):
        """Test handling of invalid JSON in message body."""
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": "invalid json {"
                }
            ]
        }
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['failed_messages'] > 0
    
    def test_missing_s3_info(self, mock_lambda_context):
        """Test handling of missing S3 information in message."""
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "Records": [{}]  # Missing s3 key
                    })
                }
            ]
        }
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['failed_messages'] > 0
    
    def test_unknown_folder_skipped(self, mock_lambda_context, set_env_vars):
        """Test that messages with unknown folders are skipped."""
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": "test-bucket"},
                                    "object": {
                                        "key": "Health_20Gizmo-08022025/Testing-1_cd037752/unknown-folder/file.csv"
                                    }
                                }
                            }
                        ]
                    })
                }
            ]
        }
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        # Should have 0 processed (skipped) and 0 failed
        assert body['processed_messages'] == 0
    
    def test_empty_records(self, mock_lambda_context):
        """Test handling of empty Records array."""
        event = {"Records": []}
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['processed_messages'] == 0
        assert body['failed_messages'] == 0
    
    @mock_aws
    def test_sqs_send_failure(self, mock_lambda_context, set_env_vars):
        """Test handling of SQS send_message failure."""
        # Create queue but then delete it to cause failure
        sqs = boto3.client('sqs', region_name='us-east-1')
        queue_url = sqs.create_queue(QueueName='test-queue')['QueueUrl']
        
        import os
        os.environ['HEALTH_HEART_RATE_QUEUE'] = 'invalid-queue-url'
        
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": "test-bucket"},
                                    "object": {
                                        "key": "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/file.csv"
                                    }
                                }
                            }
                        ]
                    })
                }
            ]
        }
        
        result = lambda_handler(event, mock_lambda_context)
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['failed_messages'] > 0
