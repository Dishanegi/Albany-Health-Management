"""
Pytest configuration and shared fixtures for testing Lambda functions and Glue jobs.
"""
import pytest
import json
import os
from unittest.mock import Mock
from moto import mock_aws
import boto3


@pytest.fixture
def mock_lambda_context():
    """Create a mock Lambda context object."""
    context = Mock()
    context.function_name = "test-function"
    context.function_version = "$LATEST"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function"
    context.memory_limit_in_mb = 256
    context.aws_request_id = "test-request-id"
    return context


@pytest.fixture
def sample_sqs_event():
    """Create a sample SQS event for testing."""
    return {
        "Records": [
            {
                "messageId": "test-message-id-1",
                "body": json.dumps({
                    "Records": [
                        {
                            "s3": {
                                "bucket": {
                                    "name": "test-source-bucket"
                                },
                                "object": {
                                    "key": "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/250204_garmin-device-heart-rate_Testing-1_cd037752.csv"
                                }
                            }
                        }
                    ]
                })
            }
        ]
    }


@pytest.fixture
def sample_sqs_event_multiple():
    """Create a sample SQS event with multiple records."""
    return {
        "Records": [
            {
                "messageId": "test-message-id-1",
                "body": json.dumps({
                    "Records": [
                        {
                            "s3": {
                                "bucket": {"name": "test-source-bucket"},
                                "object": {
                                    "key": "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/250204_garmin-device-heart-rate_Testing-1_cd037752.csv"
                                }
                            }
                        }
                    ]
                })
            },
            {
                "messageId": "test-message-id-2",
                "body": json.dumps({
                    "Records": [
                        {
                            "s3": {
                                "bucket": {"name": "test-source-bucket"},
                                "object": {
                                    "key": "Health_20Gizmo-08022025/Testing-2_ab123456/garmin-connect-sleep-stage/250205_sleep_Testing-2_ab123456.csv"
                                }
                            }
                        }
                    ]
                })
            }
        ]
    }


@pytest.fixture
def sample_lambda_sqs_event():
    """Create a sample Lambda event from SQS (for processing lambdas)."""
    return {
        "Records": [
            {
                "messageId": "test-message-id",
                "body": json.dumps({
                    "source_bucket": "test-source-bucket",
                    "object_key": "Health_20Gizmo-08022025/Testing-1_cd037752/garmin-device-heart-rate/250204_garmin-device-heart-rate_Testing-1_cd037752.csv",
                    "folder_name": "garmin-device-heart-rate"
                })
            }
        ]
    }


@pytest.fixture
def sample_heart_rate_csv():
    """Sample heart rate CSV content."""
    return """Header line 1
Header line 2
Header line 3
Header line 4
Header line 5
Header line 6
isoDate,unixTimestampInMs,heartRate,status,deviceType,timezone
2025-01-01T10:00:00Z,1704110400000,72,VALID,DEVICE_TYPE,UTC
2025-01-01T10:01:00Z,1704110460000,75,VALID,DEVICE_TYPE,UTC
"""


@pytest.fixture
def sample_step_csv():
    """Sample step CSV content."""
    return """Header line 1
Header line 2
Header line 3
Header line 4
Header line 5
Header line 6
isoDate,unixTimestampInMs,steps,durationInMs,deviceType,timezone
2025-01-01T10:00:00Z,1704110400000,100,60000,DEVICE_TYPE,UTC
2025-01-01T10:01:00Z,1704110460000,150,60000,DEVICE_TYPE,UTC
"""


@pytest.fixture
def sample_sleep_csv():
    """Sample sleep CSV content."""
    return """Header line 1
Header line 2
Header line 3
Header line 4
Header line 5
Header line 6
isoDate,unixTimestampInMs,type,durationInMs,timezoneOffsetInMs
2025-01-01T22:00:00Z,1704142800000,light,1800000,0
2025-01-01T22:30:00Z,1704144600000,deep,3600000,0
"""


@pytest.fixture
@mock_aws
def mock_s3_bucket():
    """Create a mock S3 bucket for testing."""
    s3 = boto3.client('s3', region_name='us-east-1')
    bucket_name = 'test-source-bucket'
    s3.create_bucket(Bucket=bucket_name)
    
    # Create destination bucket
    dest_bucket = 'test-destination-bucket'
    s3.create_bucket(Bucket=dest_bucket)
    
    return {
        's3_client': s3,
        'source_bucket': bucket_name,
        'destination_bucket': dest_bucket
    }


@pytest.fixture
@mock_aws
def mock_sqs_queue():
    """Create mock SQS queues for testing."""
    sqs = boto3.client('sqs', region_name='us-east-1')
    
    queues = {}
    queue_names = [
        'health-heart-rate-queue',
        'health-sleep-queue',
        'health-step-queue',
        'health-others-queue'
    ]
    
    for queue_name in queue_names:
        response = sqs.create_queue(QueueName=queue_name)
        queues[queue_name] = response['QueueUrl']
    
    return {
        'sqs_client': sqs,
        'queues': queues
    }


@pytest.fixture
def set_env_vars(monkeypatch):
    """Set environment variables for testing."""
    env_vars = {
        'HEALTH_HEART_RATE_QUEUE': 'https://sqs.us-east-1.amazonaws.com/123456789012/health-heart-rate-queue',
        'HEALTH_SLEEP_QUEUE': 'https://sqs.us-east-1.amazonaws.com/123456789012/health-sleep-queue',
        'HEALTH_STEP_QUEUE': 'https://sqs.us-east-1.amazonaws.com/123456789012/health-step-queue',
        'HEALTH_OTHERS_QUEUE': 'https://sqs.us-east-1.amazonaws.com/123456789012/health-others-queue',
        'DESTINATION_BUCKET': 'test-destination-bucket'
    }
    
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    
    return env_vars
