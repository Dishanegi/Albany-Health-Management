# Albany Health Management Platform

> **Multi-Environment Support**: Supports deploying to multiple environments (dev, sandbox, uat, qe, staging, prod) with complete resource isolation. All resources are automatically named with environment-specific suffixes. See [Deployment Section](#deploying-the-system) for details.

---

## What This System Does

This platform automatically processes two categories of data:

1. **Garmin Health Metrics** — heart rate, sleep, step count, stress, respiration, and pulse-ox data from Garmin devices
2. **Survey / Questionnaire Data** — patient questionnaire responses

Both pipelines ingest raw files, clean and normalize them, track batch completion using DynamoDB, and trigger downstream Glue jobs or merge operations once all expected files for a patient have arrived.

---

## Pipelines Overview

### Garmin Health Metrics Pipeline

```
Source bucket: albanyhealthsource-s3bucket-{env}
  {patient_id}/{data_type}/{filename}.csv
         ↓  S3 event → Main SQS
  main-router lambda  (routes by data type)
         ↓  type-specific SQS queues
  heart-rate / sleep / step / other-metrics lambdas
  (clean & reformat, write to processed bucket)
  If empty/invalid → writes zero-byte placeholder so batch is not stalled
         ↓  S3 event on processed bucket → processing-files SQS
  data-inactivity-checker lambda
  (atomically counts files in DynamoDB; expected count read from source bucket)
         ↓  when actual == expected for all data types
  EventBridge → Garmin Health Metrics Glue Workflow
              → BBI Glue Workflow
```

### Survey Data Pipeline

```
Source bucket: albanyhealthsource-s3bucket-{env}
  {patient_id}/questionnaire/{form_filename}
         ↓  S3 event → Main SQS → survey-data-file SQS
  survey-data-normalise lambda
  (cleans CSV, extracts patient_id, writes to processing bucket)
  If empty/invalid → writes zero-byte placeholder so batch is not stalled
         ↓  written to: albanyhealthsurveydataprocessing-s3bucket-{env}
              {patient_id}/questionnaire/{form_filename}_cleaned.csv
         ↓  S3 event → survey-processing-files SQS
  survey-batch-checker lambda
  (atomically counts files in DynamoDB; expected count read from source bucket)
         ↓  when actual == expected
  EventBridge → survey-data-merged-files lambda
  (reads all cleaned CSVs per patient, merges into one file)
         ↓  written to: albanyhealthsurveydatamerged-s3bucket-{env}
  EventBridge → survey-data-delete-processing-files lambda
  (cleans up processing bucket after merge)
```

---

## System Components

### Storage Buckets (S3)

| Bucket | Purpose |
|--------|---------|
| `albanyhealthsource-s3bucket-{env}` | Raw incoming files (Garmin + survey) |
| `albanyhealthprocessed-s3bucket-{env}` | Cleaned Garmin metric files |
| `albanyhealthmerged-s3bucket-{env}` | Final merged Garmin data |
| `albanyhealthbbiprocessing-s3bucket-{env}` | BBI data during processing |
| `albanyhealthbbimerged-s3bucket-{env}` | Final merged BBI data |
| `albanyhealthsurveydataprocessing-s3bucket-{env}` | Normalized survey files awaiting merge |
| `albanyhealthsurveydatamerged-s3bucket-{env}` | Final merged survey data |

All buckets have `RemovalPolicy.DESTROY` and `auto_delete_objects=True` so they are fully cleaned up on `cdk destroy`.

### Message Queues (SQS)

Each queue has a paired Dead-Letter Queue (DLQ) retaining failed messages for 14 days.

| Queue | Trigger |
|-------|---------|
| `AlbanyHealthMain-SQSQueue-{ENV}` | Source bucket → main router |
| `AlbanyHealthHeartRate-SQSQueue-{ENV}` | Heart rate files |
| `AlbanyHealthSleep-SQSQueue-{ENV}` | Sleep files |
| `AlbanyHealthStep-SQSQueue-{ENV}` | Step files |
| `AlbanyHealthOthers-SQSQueue-{ENV}` | Stress / respiration / pulse-ox files |
| `AlbanyHealthSuccessfullProcessingFiles-SQSQueue-{ENV}` | Processed bucket → inactivity checker |
| `AlbanyHealthSurveyDataProcessingFile-SQSQueue-{ENV}` | Survey files → normalise lambda |
| `AlbanyHealthSurveyProcessingFiles-SQSQueue-{ENV}` | Survey processing bucket → batch checker |

### Lambda Functions

| Function | Purpose |
|----------|---------|
| `albanyHealth-main-router-lambda-function-{env}` | Routes incoming files to the correct SQS queue based on data type |
| `albanyHealth-heart-rate-lambda-function-{env}` | Cleans and reformats heart rate CSV files |
| `albanyHealth-sleep-lambda-function-{env}` | Cleans sleep data; expands to minute-by-minute records |
| `albanyHealth-step-lambda-function-{env}` | Cleans step data; deduplicates rows |
| `albanyHealth-other-metrics-lambda-function-{env}` | Cleans stress, respiration, and pulse-ox data |
| `albanyHealth-data-inactivity-checker-lambda-function-{env}` | Tracks Garmin batch completion in DynamoDB; triggers EventBridge when all files arrive |
| `albanyHealth-survey-data-normalise-lambda-function-{env}` | Cleans survey CSVs; extracts patient ID from preamble; writes to processing bucket |
| `albanyHealth-survey-batch-checker-lambda-function-{env}` | Tracks survey batch completion in DynamoDB; triggers EventBridge when all questionnaire files are processed |
| `albanyHealth-survey-data-merged-files-lambda-function-{env}` | Merges all cleaned survey CSVs per patient into a single file |
| `albanyHealth-survey-data-delete-processing-files-lambda-function-{env}` | Deletes normalized survey files from the processing bucket after merge |
| `albanyHealth-activate-garmin-triggers-lambda-function-{env}` | Activates Garmin Glue workflow conditional triggers after deployment |
| `albanyHealth-activate-bbi-triggers-lambda-function-{env}` | Activates BBI Glue workflow conditional triggers after deployment |

All Lambda functions have explicit CloudWatch Log Groups created as CDK resources with `RemovalPolicy.DESTROY` and 1-week log retention. This ensures log groups are fully deleted on `cdk destroy`.

### DynamoDB Tables

| Table | Purpose | Partition Key |
|-------|---------|---------------|
| `AlbanyHealthBatchTracking-{env}` | Tracks Garmin file counts per patient per day | `patient_id` (e.g. `Testing-1_cd037752#2025-04-27`) |
| `AlbanyHealthSurveyBatchTracking-{env}` | Tracks survey questionnaire file counts per patient per day | `patient_id` (e.g. `Testing-1_cd037752#2025-04-27`) |

Both tables use `RemovalPolicy.DESTROY` and pay-per-request billing.

**How batch tracking works:**

- Each file arrival triggers an atomic `UpdateItem` with `ADD counter :1` and `ADD processed_files :file_id`
- A `ConditionExpression: NOT contains(processed_files, :file_id)` prevents double-counting (deduplication)
- The expected count per file type is read from the source bucket on first arrival and stored with `if_not_exists` — subsequent files skip the S3 list call
- Once actual count ≥ expected count, a second conditional write claims the trigger role (`status: processing → complete`), ensuring exactly one Lambda fires the EventBridge event even under concurrent execution

### Glue Jobs

**Garmin Health Metrics Flow**:
1. `AlbanyHealthGarmin-Data-Preprocessor-Glue-Job-{env}` — reads per patient/data-type folders from the processed bucket; supports multiple files per folder (including multiple sleep files)
2. `AlbanyHealthGarmin-Data-Merge-Glue-Job-{env}` — merges preprocessed data into patient-level files
3. `AlbanyHealthGarmin-Data-Delete-Glue-Job-{env}` — removes intermediate files after merge

**BBI Flow**:
1. `AlbanyHealthGarmin-BBI-Preprocessor-Glue-Job-{env}`
2. `AlbanyHealthGarmin-BBI-Merge-Data-Glue-Job-{env}`
3. `AlbanyHealthGarmin-BBI-Delete-Data-Glue-Job-{env}`
4. `AlbanyHealthGarmin-BBI-Delete-Source-Folders-Glue-Job-{env}`

### EventBridge Rules

| Rule | Fired by | Triggers |
|------|----------|---------|
| `trigger-merge-patient-health-metrics-{env}` | inactivity-checker (Garmin batch complete) | Garmin Health Metrics Glue Workflow |
| `trigger-patients-bbi-flow-{env}` | inactivity-checker (BBI batch complete) | BBI Glue Workflow |
| `trigger-survey-data-merged-files-{env}` | survey-batch-checker (survey batch complete) | survey-data-merged-files lambda |
| `trigger-survey-delete-processing-files-{env}` | survey-data-merged-files (merge complete) | survey-data-delete-processing-files lambda |

---

## S3 File Path Formats

### Garmin Health Metrics
```
Source:    {patient_id}/{data_type}/{filename}.csv
           e.g. Testing-1_cd037752/garmin-device-heart-rate/250204_heartrate_Testing-1_cd037752.csv

Processed: {patient_id}/{data_type}/{filename}.csv  (same key, different bucket)
```

Supported `data_type` values: `garmin-device-heart-rate`, `garmin-device-stress`, `garmin-device-step`, `garmin-device-respiration`, `garmin-device-pulse-ox`, `garmin-connect-sleep-stage`

### Survey / Questionnaire
```
Source:     {patient_id}/questionnaire/{form_filename}
            e.g. Testing-1_cd037752/questionnaire/Physical-Capacity-Aw_6274612c

Processing: {patient_id}/questionnaire/{form_filename}_cleaned.csv
            e.g. Testing-1_cd037752/questionnaire/Physical-Capacity-Aw_6274612c_cleaned.csv

Merged:     {patient_id}/{earliest_date}_merged.csv
            e.g. Testing-1_cd037752/20260101_merged.csv
```

---

## Empty / Invalid File Handling

Some source files contain only header rows or labels with no actual data. This would cause processing lambdas to throw `No columns to parse from file` and write nothing to the destination bucket, stalling the batch indefinitely.

**All processing lambdas** (heart rate, sleep, step, other metrics, survey normalise) now handle this by:
1. Detecting empty DataFrames or parse errors
2. Writing a **zero-byte placeholder** file at the same destination key
3. Logging a warning instead of failing

The batch checker counts the placeholder file normally, so the batch still completes. The Glue preprocessing scripts and survey merge lambda skip zero-byte files when reading back.

---

## Getting Started

### Prerequisites
- Python 3.13 or higher
- AWS Account with appropriate permissions
- AWS CDK installed

### Initial Setup

```bash
cd infra
python3 -m venv .venv
source .venv/bin/activate        # Mac/Linux
# .venv\Scripts\activate.bat     # Windows
pip install -r requirements.txt
```

### Deploying the System

```bash
# First-time setup (once per AWS account/region)
cdk bootstrap

# Preview changes
cdk diff --context env=dev

# Deploy
cdk deploy --context env=dev       # Development (default)
cdk deploy --context env=sandbox   # Sandbox
cdk deploy --context env=staging   # Staging
cdk deploy --context env=prod      # Production

# Deploy with explicit account and region
cdk deploy --context env=staging --context account=123456789012 --context region=us-east-1

# Destroy (removes all resources including log groups)
cdk destroy --context env=dev
```

**Available environments**: `dev`, `sandbox`, `sandox`, `uat`, `qe`, `staging`, `prod`

---

## Project Structure

```
Albany-Health-Management/
│
├── infra/
│   ├── app.py
│   ├── cdk.json
│   ├── DEPLOYMENT.md
│   └── albany_health_management/
│       ├── config.py
│       ├── albany_health_management_stack.py
│       └── constructs/
│           ├── s3_buckets.py         — 7 S3 buckets (Garmin + Survey)
│           ├── sqs_queues.py         — 8 SQS queues + DLQs
│           ├── dynamodb_table.py     — Batch tracking tables (Garmin + Survey)
│           ├── lambda_functions.py   — 12 Lambda functions with explicit log groups
│           ├── glue_jobs.py          — 7 Glue jobs
│           ├── glue_workflows.py     — Garmin + BBI Glue workflows
│           └── eventbridge_rules.py  — 4 EventBridge rules
│
└── services/
    ├── ingestion/
    │   └── lambdas/
    │       ├── albanyHealth-main-router-lambda-function/
    │       ├── albanyHealth-heart-rate-lambda-function/
    │       ├── albanyHealth-sleep-lambda-function/
    │       ├── albanyHealth-step-lambda-function/
    │       ├── albanyHealth-other-metrics-lambda-function/
    │       ├── albanyHealth-data-inactivity-checker-lambda-function/
    │       ├── albanyHealth-survey-data-normalise-lambda-function/
    │       ├── albanyHealth-survey-batch-checker-lambda-function/
    │       ├── albanyHealth-survey-data-merged-files-lambda-function/
    │       ├── albanyHealth-survey-data-delete-processing-files-lambda-function/
    │       ├── albanyHealth-activate-garmin-triggers-lambda-function/
    │       └── albanyHealth-activate-bbi-triggers-lambda-function/
    └── analytics/
        └── glue_jobs/
            ├── AlbanyHealth-Garmin-Health-metrics-Flow-Script/
            └── AlbanyHealth-BBI-Flow-Script/
```

---

## Monitoring & Troubleshooting

### Where to Check

| Console | What to check |
|---------|--------------|
| **CloudWatch Logs** | Lambda function logs — each function has its own log group `/aws/lambda/{function-name}-{env}` |
| **DynamoDB** | `AlbanyHealthBatchTracking-{env}` and `AlbanyHealthSurveyBatchTracking-{env}` — check `questionnaire_count`, `expected_questionnaire_count`, `status` |
| **SQS** | Queue depth — messages stuck in DLQ indicate repeated Lambda failures |
| **Glue** | Job run history under `AlbanyHealthGarminHealthMetricsWorkflow-{env}` |
| **S3** | Source bucket for incoming files; processing bucket for survey normalized files |

### Common Issues

**Batch never triggers (EventBridge never fires)**
- Check DynamoDB item for the patient: actual count should equal expected count
- If expected count is 0, the source bucket count returned 0 — verify the source bucket path matches `{patient_id}/{data_type}/` for Garmin or `{patient_id}/questionnaire/` for survey
- If some files are empty/label-only, check that placeholder files were written to the processed/processing bucket (look for 0-byte files in the destination bucket)

**"No columns to parse from file" in logs**
- Expected — the lambda detected an empty/headers-only source file
- A zero-byte placeholder should have been written to the destination bucket
- The batch will still complete once all files (including placeholders) are counted

**Survey batch checker: "does not match survey pattern"**
- The file key in the processing bucket does not follow `{patient_id}/questionnaire/{filename}` format
- Check the survey-data-normalise lambda is writing with the correct path structure
- Verify the source file was under `{patient_id}/questionnaire/` in the source bucket

**Files not processed after `cdk destroy` + redeploy**
- DynamoDB items from a previous batch may persist if they were not cleaned up
- The tables are destroyed with `cdk destroy` — this is expected behaviour

---

## Resource Cleanup (cdk destroy)

All resources are configured to be fully deleted on `cdk destroy`:

| Resource type | Cleanup mechanism |
|---------------|------------------|
| S3 buckets | `RemovalPolicy.DESTROY` + `auto_delete_objects=True` |
| DynamoDB tables | `RemovalPolicy.DESTROY` |
| Lambda functions | `RemovalPolicy.DESTROY` |
| Lambda log groups | Explicit `logs.LogGroup` with `RemovalPolicy.DESTROY` (1-week retention) |
| SQS queues + DLQs | CDK v2 default (DESTROY) |
| Glue jobs | CloudFormation default (DELETE) |
| EventBridge rules / IAM roles | CloudFormation default (DELETE) |

---

## Security Notes

- **Environment Isolation**: Each environment has separate IAM roles and resources
- **Least Privilege**: Lambda functions are granted only the specific S3/DynamoDB/SQS/EventBridge permissions they need
- **DynamoDB Atomic Operations**: Batch counting uses conditional writes to prevent race conditions under concurrent Lambda execution
- **Log Retention**: All Lambda log groups retain logs for 1 week and are deleted on stack destroy
- **Multi-Account Support**: Can deploy different environments to different AWS accounts

---

## Version Information

- **CDK Version**: 2.x
- **Python Version**: 3.13+
- **Glue Version**: 5.0
- **Lambda Runtime**: Python 3.13 (Python 3.11 for trigger-activator functions)