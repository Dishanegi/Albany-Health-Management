# Multi-Environment Deployment Guide

This CDK application now supports deploying to multiple environments (dev, sandbox, sandox, uat, qe, staging, prod).

## Quick Start

### Deploy to Development (default)
```bash
cd infra
cdk deploy
# or explicitly:
cdk deploy --context env=dev
```

### Deploy to Sandbox
```bash
cdk deploy --context env=sandbox
```

### Deploy to Sandox
```bash
cdk deploy --context env=sandox
```

### Deploy to UAT
```bash
cdk deploy --context env=uat
```

### Deploy to QE
```bash
cdk deploy --context env=qe
```

### Deploy to Staging
```bash
cdk deploy --context env=staging
```

### Deploy to Production
```bash
cdk deploy --context env=prod
```

## Specifying AWS Account and Region

### Using CDK Context
```bash
# Deploy to specific account and region
cdk deploy --context env=staging --context account=123456789012 --context region=us-west-2
```

### Using Environment Variables
```bash
export CDK_ENV=staging
export CDK_DEFAULT_ACCOUNT=123456789012
export CDK_DEFAULT_REGION=us-west-2
cdk deploy
```

## Environment-Specific Resource Naming

All resources are automatically named with the environment suffix:
- **S3 Buckets**: `albanyhealthsource-s3bucket-{env}`
- **Lambda Functions**: `albanyHealth-*-lambda-function-{env}`
- **SQS Queues**: `AlbanyHealth*-SQSQueue-{ENV}`
- **Glue Workflows**: `AlbanyHealthGarminHealthMetricsWorkflow-{env}`
- **EventBridge Rules**: `trigger-*-{env}`

## Available Environments

- `dev` - Development environment (default)
- `sandbox` - Sandbox environment for testing
- `sandox` - Sandox environment
- `uat` - User Acceptance Testing environment
- `qe` - Quality Engineering environment
- `staging` - Staging environment
- `prod` - Production environment

## Configuration

Environment configurations are defined in `albany_health_management/config.py`. You can extend this file to add environment-specific settings like:
- Different resource sizes
- Different retention policies
- Environment-specific feature flags

## Stack Names

The stack name includes the environment: `AlbanyHealthManagementStack-{env}`

This allows you to deploy multiple environments in the same AWS account if needed.

## Tags

All resources are automatically tagged with:
- `Environment`: The environment name (dev/sandbox/sandox/uat/qe/staging/prod)
- `Project`: AlbanyHealthManagement

## Troubleshooting

### Error: ResourceExistenceCheck Failed

If you see an error like:
```
FAILED: The following hook(s)/validation failed: [AWS::EarlyValidation::ResourceExistenceCheck]
```

This usually means:

1. **S3 Bucket Names Already Exist**: S3 bucket names are globally unique across all AWS accounts. If a bucket name already exists (even in another account), you'll get this error.

   **Solution**: 
   - Check if the buckets already exist: `aws s3 ls | grep albanyhealth`
   - If they exist in your account, you may need to delete them first or use different names
   - Consider adding a unique prefix to bucket names (e.g., account ID or project prefix)

2. **Account/Region Not Configured**: If account or region is not properly set, CloudFormation validation may fail.

   **Solution**:
   - Explicitly set account and region: `cdk deploy --context env=sandbox --context account=YOUR_ACCOUNT_ID --context region=us-east-1`
   - Or configure AWS CLI: `aws configure`

3. **Stack Already Exists with Different Configuration**: If a stack with the same name exists but with different account/region settings.

   **Solution**:
   - Check existing stacks: `aws cloudformation list-stacks`
   - Delete the existing stack if needed: `cdk destroy --context env=sandbox`

### Check Stack Name

The stack name automatically includes the environment: `AlbanyHealthManagementStack-{env}`

To verify:
```bash
cdk synth --context env=sandbox | grep "StackName"
```

### Verify Resource Names

Before deploying, you can check what resource names will be created:
```bash
cdk synth --context env=sandbox
```

This will show you all the resource names that will be created, including S3 bucket names.

