# Testing Guide for Albany Health Management

This directory contains unit, integration, and infrastructure tests for Lambda functions, Glue jobs, and AWS resources.

## Table of Contents

1. [Setup](#setup)
2. [How to Run Tests](#how-to-run-tests)
3. [Test Structure](#test-structure)
4. [Test Categories](#test-categories)
5. [Understanding Test Results](#understanding-test-results)
6. [Writing Tests](#writing-tests)
7. [Test Fixtures](#test-fixtures)
8. [Coverage Reports](#coverage-reports)
9. [Best Practices](#best-practices)
10. [Troubleshooting](#troubleshooting)

## Setup

### Prerequisites

- Python 3.12+
- pip package manager

### Install Dependencies

```bash
# From project root directory
cd /Users/dishanegi/Desktop/Masters/iConsult/Albany-Health-Management

# Install all testing dependencies
pip install -r requirements-dev.txt

# Or install testing dependencies directly
pip install pytest pytest-cov moto boto3 pandas pytest-mock aws-cdk-lib
```

### Verify Installation

```bash
# Check pytest is installed
pytest --version

# Check all dependencies
pip list | grep -E "pytest|moto|boto3|pandas"
```

## How to Run Tests

### Quick Start

```bash
# Run all tests (recommended for first-time setup verification)
pytest tests/ -v

# Run all tests without coverage (faster)
pytest tests/ -v --no-cov

# Run all tests and show print statements
pytest tests/ -v -s
```

### Run All Tests

```bash
# From project root
pytest tests/ -v

# With detailed output
pytest tests/ -vv

# With print statements visible
pytest tests/ -v -s
```

### Run Tests with Coverage

```bash
# Generate coverage report for services
pytest tests/ --cov=services --cov-report=html

# View coverage report
open htmlcov/index.html  # macOS
# or
xdg-open htmlcov/index.html  # Linux

# Generate coverage report in terminal
pytest tests/ --cov=services --cov-report=term-missing
```

### Run Specific Test Category

```bash
# Unit tests (CDK stack tests)
pytest tests/unit/ -v

# Integration tests (Lambda function tests)
pytest tests/integration/ -v

# Infrastructure tests (AWS resource validation)
pytest tests/infrastructure/ -v --no-cov
```

### Run Specific Test File

```bash
# Main router Lambda tests
pytest tests/integration/test_lambda_main_router.py -v

# Heart rate Lambda tests
pytest tests/integration/test_lambda_heart_rate.py -v

# Step Lambda tests
pytest tests/integration/test_lambda_step.py -v

# Sleep Lambda tests
pytest tests/integration/test_lambda_sleep.py -v

# CDK stack tests
pytest tests/unit/test_albany_health_management_stack.py -v

# Infrastructure resource tests
pytest tests/infrastructure/test_infrastructure_resources.py -v --no-cov
```

### Run Specific Test Class

```bash
# Run all tests in a specific test class
pytest tests/integration/test_lambda_heart_rate.py::TestExtractParticipantId -v

# Run all tests in TestProcessHeartRateData class
pytest tests/integration/test_lambda_heart_rate.py::TestProcessHeartRateData -v
```

### Run Specific Test Function

```bash
# Run a single test function
pytest tests/integration/test_lambda_main_router.py::TestExtractFolderName::test_valid_path_three_segments -v

# Run a test with real data
pytest tests/integration/test_lambda_heart_rate.py::TestHeartRateWithRealData::test_lambda_handler_with_real_file -v -s
```

### Run Tests by Pattern

```bash
# Run all tests matching a pattern
pytest tests/integration/ -k "heart_rate" -v

# Run all tests with "real_data" in the name
pytest tests/integration/ -k "real_data" -v -s

# Run all tests except those matching a pattern
pytest tests/integration/ -k "not real_data" -v
```

### Run Tests in Parallel (Faster)

```bash
# Install pytest-xdist first
pip install pytest-xdist

# Run tests in parallel (4 workers)
pytest tests/ -n 4 -v
```

### Run Tests with Markers

```bash
# Run only unit tests (if markers are configured)
pytest -m unit -v

# Run only integration tests
pytest -m integration -v

# Run tests marked as slow
pytest -m "not slow" -v
```

## Test Structure

```
tests/
├── conftest.py                      # Shared fixtures and configuration
├── fixtures/                        # Test data files
│   └── real_data/                   # Real CSV files for integration tests
│       ├── 251119_garmin-device-heart-rate_Testing-1_cd037752.csv
│       ├── 251119_garmin-device-step_Testing-1_cd037752.csv
│       └── 251101_garmin-connect-sleep-stage_Testing-1_cd037752.csv
├── unit/                            # Unit tests
│   └── test_albany_health_management_stack.py  # CDK stack tests
├── integration/                    # Integration tests
│   ├── test_lambda_main_router.py   # Main router Lambda tests
│   ├── test_lambda_heart_rate.py    # Heart rate Lambda tests
│   ├── test_lambda_step.py          # Step Lambda tests
│   └── test_lambda_sleep.py         # Sleep Lambda tests
└── infrastructure/                  # Infrastructure tests
    ├── test_infrastructure_resources.py  # AWS resource validation tests
    └── RESOURCE_COUNTS.md           # Resource counts documentation
```

## Test Categories

### Unit Tests (`tests/unit/`)

**Purpose**: Test CDK stack definitions and configurations

**Scope**: Isolated tests for stack resources

**Files**:
- `test_albany_health_management_stack.py` - Validates CDK stack creation

**Example Command**:
```bash
pytest tests/unit/test_albany_health_management_stack.py -v
```

### Integration Tests (`tests/integration/`)

**Purpose**: Test Lambda functions with AWS service interactions

**Scope**: Full Lambda handler tests with mocked S3/SQS and real CSV data

**Includes**:
- Function-level unit tests (individual function testing)
- Lambda handler integration tests (full handler with mocked AWS services)
- Real data processing tests (using actual CSV files)

**Files**:
- `test_lambda_main_router.py` - Main router Lambda (routing logic)
- `test_lambda_heart_rate.py` - Heart rate Lambda (data processing)
- `test_lambda_step.py` - Step Lambda (data processing with deduplication)
- `test_lambda_sleep.py` - Sleep Lambda (data processing with expansion)

**Example Commands**:
```bash
# Run all integration tests
pytest tests/integration/ -v

# Run heart rate tests with real data
pytest tests/integration/test_lambda_heart_rate.py::TestHeartRateWithRealData -v -s

# Run step tests with deduplication
pytest tests/integration/test_lambda_step.py::TestStepWithRealData::test_deduplication_with_real_data -v -s
```

### Infrastructure Tests (`tests/infrastructure/`)

**Purpose**: Validate AWS resources are created correctly

**Scope**: CDK assertions for resource counts, naming conventions, and properties

**Files**:
- `test_infrastructure_resources.py` - Validates all AWS resources

**What It Tests**:
- Exact resource counts (SQS queues, Lambda functions, S3 buckets, etc.)
- Exact naming conventions
- Resource properties (timeouts, memory, configurations)
- Environment-specific naming

**Example Command**:
```bash
pytest tests/infrastructure/test_infrastructure_resources.py -v --no-cov
```

## Understanding Test Results

### Successful Test Run

```
============================= test session starts ==============================
platform darwin -- Python 3.12.4, pytest-9.0.2
collected 87 items

tests/integration/test_lambda_main_router.py::TestExtractFolderName::test_valid_path_three_segments PASSED [  1%]
tests/integration/test_lambda_heart_rate.py::TestExtractParticipantId::test_valid_participant_id PASSED [  2%]
...

============================== 87 passed in 6.74s ==============================
```

### Failed Test Example

```
FAILED tests/integration/test_lambda_heart_rate.py::TestExtractParticipantId::test_participant_id_mixed_case
AssertionError: assert None == 'AbC12345'
```

**What to do**: Check the test output for details, verify the function logic matches expected behavior.

### Skipped Test Example

```
SKIPPED tests/integration/test_lambda_heart_rate.py::TestHeartRateWithRealData::test_real_file_exists
Reason: Real CSV file not found: tests/fixtures/real_data/251119_garmin-device-heart-rate_Testing-1_cd037752.csv
```

**What to do**: Place the required CSV file in `tests/fixtures/real_data/` to enable the test.

### Test Output Interpretation

- **PASSED** ✅ - Test completed successfully
- **FAILED** ❌ - Test assertion failed, check the error message
- **SKIPPED** ⏭️ - Test was skipped (usually due to missing dependencies or data)
- **ERROR** ⚠️ - Test encountered an error before assertions (check imports, setup)

## Writing Tests

### Example: Testing a Lambda Function

```python
import pytest
from moto import mock_aws
import boto3
import json
from main import lambda_handler

@mock_aws
def test_lambda_handler():
    # Setup - Create mock S3 bucket
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='test-source-bucket')
    
    # Create test CSV content
    csv_content = "isodate,value\n2024-01-01T00:00:00Z,72"
    s3.put_object(
        Bucket='test-source-bucket',
        Key='test-file_cd037752.csv',
        Body=csv_content
    )
    
    # Create test event (SQS event format)
    event = {
        "Records": [{
            "body": json.dumps({
                "Records": [{
                    "s3": {
                        "bucket": {"name": "test-source-bucket"},
                        "object": {"key": "test-file_cd037752.csv"}
                    }
                }]
            })
        }]
    }
    
    # Execute
    result = lambda_handler(event, mock_lambda_context)
    
    # Assert
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['processed_files'] == 1
```

### Example: Testing Data Processing

```python
import pandas as pd
from main import process_heart_rate_data

def test_process_heart_rate_data():
    # Setup - Create test DataFrame
    input_df = pd.DataFrame({
        'isodate': ['2024-01-01T00:00:00Z', '2024-01-01T00:01:00Z'],
        'value': [72, 75]
    })
    
    # Execute
    participant_id = 'cd037752'
    result_df = process_heart_rate_data(input_df, participant_id)
    
    # Assert
    assert 'participant_id' in result_df.columns
    assert 'timestamp' in result_df.columns
    assert 'participantid_timestamp' in result_df.columns
    assert len(result_df) == 2
    assert all(result_df['participant_id'] == participant_id)
```

### Example: Testing with Real Data

```python
from pathlib import Path
import pandas as pd
from main import process_heart_rate_data

REAL_CSV_FILE = Path(__file__).parent.parent / "fixtures" / "real_data" / "file.csv"

def test_with_real_data():
    if not REAL_CSV_FILE.exists():
        pytest.skip(f"Real CSV file not found: {REAL_CSV_FILE}")
    
    # Read real CSV file
    df = pd.read_csv(REAL_CSV_FILE, skiprows=6, encoding='utf-8')
    
    # Process with real data
    participant_id = extract_participant_id(REAL_CSV_FILE.name)
    processed_df = process_heart_rate_data(df, participant_id)
    
    # Assert on real data
    assert len(processed_df) > 0
    assert 'participant_id' in processed_df.columns
```

## Test Fixtures

Common fixtures are defined in `conftest.py` and can be used in any test:

### Available Fixtures

- `mock_lambda_context` - Mock Lambda context object
- `sample_sqs_event` - Sample SQS event structure
- `sample_lambda_sqs_event` - Sample Lambda SQS event structure
- `sample_heart_rate_csv` - Sample CSV content for heart rate data
- `mock_s3_bucket` - Mock S3 bucket setup (with `@mock_aws`)
- `mock_sqs_queue` - Mock SQS queues setup (with `@mock_aws`)
- `set_env_vars` - Set environment variables for tests

### Using Fixtures

```python
def test_with_fixture(mock_lambda_context, set_env_vars):
    # Use the fixture
    result = lambda_handler(event, mock_lambda_context)
    assert result is not None
```

## Coverage Reports

### Generate Coverage Report

```bash
# Generate HTML coverage report
pytest tests/ --cov=services --cov-report=html

# Generate terminal coverage report
pytest tests/ --cov=services --cov-report=term-missing

# Generate XML coverage report (for CI/CD)
pytest tests/ --cov=services --cov-report=xml
```

### View Coverage Report

```bash
# macOS
open htmlcov/index.html

# Linux
xdg-open htmlcov/index.html

# Windows
start htmlcov/index.html
```

### Coverage Report Interpretation

- **Coverage Percentage**: Shows how much of your code is tested
- **Missing Lines**: Lines not covered by tests (shown in red)
- **Covered Lines**: Lines covered by tests (shown in green)
- **Target**: Aim for 80%+ coverage for critical functions

## Best Practices

1. **Isolate Tests**: Each test should be independent and not rely on other tests
2. **Use Mocks**: Mock AWS services (S3, SQS) using `moto` to avoid real AWS calls
3. **Test Edge Cases**: Include tests for error conditions, empty inputs, invalid data
4. **Keep Tests Fast**: Unit tests should run quickly (< 1 second each)
5. **Clear Assertions**: Make assertions clear and specific with descriptive messages
6. **Test Data**: Use realistic but minimal test data for unit tests
7. **Real Data Tests**: Use actual CSV files for integration tests to validate real-world scenarios
8. **Descriptive Names**: Use clear test function names that describe what is being tested
9. **One Assertion Per Concept**: Test one concept per test function
10. **Clean Up**: Tests should clean up after themselves (mocks handle this automatically)

## Troubleshooting

### Import Errors

**Problem**: `ModuleNotFoundError` or `ImportError`

**Solution**: Ensure Lambda function paths are added to `sys.path` in the test file:
```python
import sys
import os
lambda_path = os.path.join(os.path.dirname(__file__), '..', '..', 'services', 'ingestion', 'lambdas', 'function-name')
if lambda_path not in sys.path:
    sys.path.insert(0, lambda_path)

# Clear cached modules
if 'main' in sys.modules:
    del sys.modules['main']
```

### Moto Issues

**Problem**: Mocks not working, real AWS calls being made

**Solution**: Ensure you're using the `@mock_aws` decorator:
```python
from moto import mock_aws

@mock_aws
def test_function():
    # Your test code
    ...
```

**Note**: `mock_aws` replaces the old `@mock_s3` and `@mock_sqs` decorators in moto 4.0+

### Environment Variables

**Problem**: Tests failing due to missing environment variables

**Solution**: Use the `set_env_vars` fixture or `monkeypatch`:
```python
def test_with_env(monkeypatch):
    monkeypatch.setenv('DESTINATION_BUCKET', 'test-bucket')
    # Your test code
```

### Missing Test Data

**Problem**: Tests skipped due to missing CSV files

**Solution**: Place CSV files in `tests/fixtures/real_data/`:
```bash
# Copy your CSV file to the fixtures folder
cp /path/to/your/file.csv tests/fixtures/real_data/

# Update the path in the test file
REAL_CSV_FILE = Path(__file__).parent.parent / "fixtures" / "real_data" / "your-file.csv"
```

### CDK Test Permission Errors

**Problem**: `EPERM: operation not permitted` when running infrastructure tests

**Solution**: Run with required permissions:
```bash
pytest tests/infrastructure/ -v --no-cov
# If still failing, may need to clear CDK cache or run with elevated permissions
```

### Slow Test Execution

**Problem**: Tests taking too long to run

**Solutions**:
1. Run tests in parallel: `pytest tests/ -n 4`
2. Run specific test files instead of all tests
3. Use `--no-cov` flag to skip coverage calculation
4. Run tests by pattern: `pytest tests/ -k "pattern"`

### Test Failures After Code Changes

**Problem**: Tests failing after modifying Lambda functions

**Solutions**:
1. Clear Python cache: `find . -type d -name __pycache__ -exec rm -r {} +`
2. Clear module cache in test: `del sys.modules['main']`
3. Verify import paths are correct
4. Check that function signatures haven't changed

## Quick Reference Commands

```bash
# Most common commands
pytest tests/ -v                          # Run all tests
pytest tests/integration/ -v              # Run integration tests
pytest tests/integration/ -v -s            # Run with print statements
pytest tests/ --cov=services --cov-report=html  # Generate coverage
pytest tests/integration/test_lambda_heart_rate.py -v  # Run specific file
pytest tests/ -k "heart_rate" -v          # Run tests matching pattern
pytest tests/ -n 4 -v                     # Run in parallel (4 workers)
```

## Additional Resources

- [pytest Documentation](https://docs.pytest.org/)
- [moto Documentation](https://docs.getmoto.org/)
- [AWS CDK Testing Guide](https://docs.aws.amazon.com/cdk/v2/guide/testing.html)
