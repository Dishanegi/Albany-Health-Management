# Multi-Environment Deployment Guide

This CDK application supports deploying to multiple environments (dev, sandbox, sandox, uat, qe, staging, prod) with complete resource isolation per environment.

---

## Quick Start

### Deploy to Development (default)
```bash
cd infra
cdk deploy
# or explicitly:
cdk deploy --context env=dev
```

### Deploy to Other Environments
```bash
cdk deploy --context env=sandbox
cdk deploy --context env=uat
cdk deploy --context env=qe
cdk deploy --context env=staging
cdk deploy --context env=prod
```

### Deploy with Explicit AWS Account and Region
```bash
cdk deploy --context env=staging --context account=123456789012 --context region=us-east-1
```

### Using Environment Variables
```bash
export CDK_ENV=staging
export CDK_DEFAULT_ACCOUNT=123456789012
export CDK_DEFAULT_REGION=us-east-1
cdk deploy
```

---

## Destroying a Stack

All resources are configured to be fully cleaned up on destroy — including S3 bucket contents, DynamoDB tables, Lambda functions, and CloudWatch log groups.

```bash
cdk destroy --context env=dev
```

See the [Resource Cleanup](#resource-cleanup) section for details on what gets deleted.

---

## Available Environments

| Environment | Purpose |
|-------------|---------|
| `dev` | Development (default) |
| `sandbox` | Isolated testing |
| `sandox` | Sandox environment |
| `uat` | User Acceptance Testing |
| `qe` | Quality Engineering |
| `staging` | Pre-production |
| `prod` | Production |

---

## Environment-Specific Resource Naming

All resources are automatically suffixed with the environment name to ensure complete isolation between environments deployed in the same AWS account.

| Resource Type | Naming Pattern |
|---------------|---------------|
| S3 Buckets | `albanyhealthsource-s3bucket-{env}` |
| Lambda Functions | `albanyHealth-*-lambda-function-{env}` |
| SQS Queues | `AlbanyHealth*-SQSQueue-{ENV}` |
| SQS DLQs | `AlbanyHealth*-DLQ-{ENV}` |
| DynamoDB Tables | `AlbanyHealthBatchTracking-{env}`, `AlbanyHealthSurveyBatchTracking-{env}` |
| Glue Jobs | `AlbanyHealthGarmin-*-Glue-Job-{env}` |
| Glue Workflows | `AlbanyHealthGarminHealthMetricsWorkflow-{env}`, `AlbanyHealthGarminBBIWorkflow-{env}` |
| EventBridge Rules | `trigger-*-{env}` |
| IAM Roles | `AlbanyHealthGlueJobRole-{env}`, `AlbanyHealthEventBridgeToGlueRole-{env}` |
| CloudWatch Log Groups | `/aws/lambda/albanyHealth-*-lambda-function-{env}` |
| CloudFormation Stack | `AlbanyHealthManagementStack-{env}` |

---

## What Gets Deployed

Each environment deploys the following AWS resources:

### S3 Buckets (7)
- `albanyhealthsource-s3bucket-{env}` — raw incoming Garmin and survey files
- `albanyhealthprocessed-s3bucket-{env}` — cleaned Garmin metric files
- `albanyhealthmerged-s3bucket-{env}` — final merged Garmin data
- `albanyhealthbbiprocessing-s3bucket-{env}` — BBI data during processing
- `albanyhealthbbimerged-s3bucket-{env}` — final merged BBI data
- `albanyhealthsurveydataprocessing-s3bucket-{env}` — normalized survey files awaiting merge
- `albanyhealthsurveydatamerged-s3bucket-{env}` — final merged survey data

### SQS Queues (8 queues + 7 DLQs)
- Main, HeartRate, Sleep, Step, Others, ProcessingFiles, SurveyDataFile, SurveyProcessingFiles

### DynamoDB Tables (2)
- `AlbanyHealthBatchTracking-{env}` — Garmin file batch tracking
- `AlbanyHealthSurveyBatchTracking-{env}` — Survey file batch tracking

### Lambda Functions (12)
- main-router, heart-rate, sleep, step, other-metrics
- data-inactivity-checker, survey-data-normalise, survey-batch-checker
- survey-data-merged-files, survey-data-delete-processing-files
- activate-garmin-triggers, activate-bbi-triggers

### Glue Jobs (7)
- Garmin: Data-Preprocessor, Data-Merge, Data-Delete
- BBI: Preprocessor, Merge-Data, Delete-Data, Delete-Source-Folders

### EventBridge Rules (4)
- `trigger-merge-patient-health-metrics-{env}`
- `trigger-patients-bbi-flow-{env}`
- `trigger-survey-data-merged-files-{env}`
- `trigger-survey-delete-processing-files-{env}`

### CloudWatch Log Groups (12)
One explicit log group per Lambda function — all with 1-week retention and `RemovalPolicy.DESTROY`.

---

## Resource Cleanup

Running `cdk destroy --context env=<env>` removes all of the following:

| Resource | Cleanup Method |
|----------|---------------|
| S3 buckets (all 7) | `RemovalPolicy.DESTROY` + `auto_delete_objects=True` — deletes all objects first |
| DynamoDB tables | `RemovalPolicy.DESTROY` |
| Lambda functions | `RemovalPolicy.DESTROY` |
| CloudWatch log groups | Explicit `logs.LogGroup` with `RemovalPolicy.DESTROY` — previously these would persist after destroy |
| SQS queues + DLQs | CDK v2 default (DESTROY) |
| Glue jobs | CloudFormation default (DELETE) |
| EventBridge rules | CloudFormation default (DELETE) |
| IAM roles | CloudFormation default (DELETE) |

---

## Tags

All resources are automatically tagged with:
- `Environment`: the environment name (e.g. `dev`, `prod`)
- `Project`: `AlbanyHealthManagement`

---

## Configuration

Environment configurations are defined in `albany_health_management/config.py`. Each environment can specify:
- AWS account and region
- Environment-specific feature flags or resource sizes

To add a new environment, add an entry to the `ENVIRONMENTS` dictionary in `config.py`.

---

## Troubleshooting

### Error: ResourceExistenceCheck Failed

```
FAILED: The following hook(s)/validation failed: [AWS::EarlyValidation::ResourceExistenceCheck]
```

**Cause 1 — S3 bucket names already exist**

S3 bucket names are globally unique. If a bucket already exists in any AWS account you will get this error.

```bash
# Check if buckets exist in your account
aws s3 ls | grep albanyhealth
```

If they exist, either delete them or re-deploy after running `cdk destroy` for the environment.

**Cause 2 — Account/region not configured**

```bash
cdk deploy --context env=sandbox --context account=YOUR_ACCOUNT_ID --context region=us-east-1
# or
aws configure
```

**Cause 3 — Stack already exists with different configuration**

```bash
aws cloudformation list-stacks --query 'StackSummaries[?contains(StackName,`AlbanyHealth`)]'
cdk destroy --context env=sandbox   # delete then redeploy
```

### Verify Resource Names Before Deploying

```bash
cdk synth --context env=sandbox
```

This shows all resource names that will be created, useful for checking for naming conflicts before a full deploy.

### Check Stack Name

```bash
cdk synth --context env=sandbox | grep "StackName"
```

Stack name format: `AlbanyHealthManagementStack-{env}`

### Log Groups Not Deleted After Previous Destroy

If you deployed before the explicit log group fix was added, old log groups may still exist. Delete them manually:

```bash
aws logs delete-log-group --log-group-name /aws/lambda/albanyHealth-main-router-lambda-function-dev
# repeat for each function name
```

After redeploying, `cdk destroy` will handle log group cleanup automatically going forward.