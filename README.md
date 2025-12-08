# Albany Health Management Platform

> **🆕 Multi-Environment Support**: This platform now supports deploying to multiple environments (dev, sandbox, uat, qe, staging, prod) with complete resource isolation. All resources are automatically named with environment-specific suffixes. See [Deployment Section](#deploying-the-system) for details.

## 📋 What This System Does

This platform automatically processes health data from Garmin devices. When new health data files arrive, the system:

1. **Receives** the data files
2. **Organizes** them by patient and data type 
3. **Processes** the data to make it usable
4. **Combines** related data together
5. **Stores** the final results for analysis

Think of it like a smart filing system that automatically sorts, processes, and organizes health data as it arrives.

---

## 🔄 How It Works (Simple Overview)

```
New Health Data Files Arrive
         ↓
    System Detects Files
         ↓
    Files Are Sorted by Type
    (Heart Rate, Sleep, Steps, etc.)
         ↓
    Data Is Processed & Cleaned 
         ↓
    Data Is Combined by Patient
         ↓  
    Final Results Are Stored
```

### Step-by-Step Process

1. **Data Arrival**: Health data files are uploaded to a source S3 bucket
2. **File Detection**: The system automatically detects when new files arrive 
3. **File Routing**: Files are sorted into different SQS queues based on their type:
   - Heart Rate data
   - Sleep data 
   - Step data
   - Other health metrics (stress, respiration, pulse-ox)
4. **Data Processing**: Lambda functions process each file type to:
   - Clean the data
   - Format it correctly
   - Add patient identification 
5. **Batch Completion Check**: The system waits until all required files for a patient are received
6. **Data Merging**: Once complete, Glue jobs combine and organize the data
7. **Final Storage**: Processed data is stored in S3 buckets for analysis

---

## 🏗️ System Components (What Each Part Does)

### 📦 Storage Buckets (S3)
**What they do**: Store health data files at different stages of processing

- **Source Bucket**: Where new data files first arrive
- **Processed Bucket**: Where cleaned and organized data is stored
- **Merged Bucket**: Where final combined data is stored
- **BBI Processing Bucket**: Stores BBI (Beat-to-Beat Interval) data during processing
- **BBI Merged Bucket**: Final storage for processed BBI data

### 📨 Message Queues (SQS) 
**What they do**: Act like waiting lines for files to be processed

- Files wait in queues until a Lambda function is ready to process them
- Different queues for different types of data (heart rate, sleep, steps, etc.)
- Ensures files are processed in order and nothing gets lost

### ⚙️ Processing Workers (Lambda Functions)
**What they do**: Process the data files

- **Main Router**: Decides which queue each file should go to
- **Heart Rate Processor**: Processes heart rate data
- **Sleep Processor**: Processes sleep data 
- **Step Processor**: Processes step data
- **Other Metrics Processor**: Processes stress, respiration, and pulse-ox data
- **Batch Checker**: Monitors when all files for a patient are received 
- **Garmin Triggers Activator**: Automatically activates Glue workflow triggers for Garmin data processing
- **BBI Triggers Activator**: Automatically activates Glue workflow triggers for BBI data processing

### 🔄 Data Processing Jobs (Glue Jobs)
**What they do**: Perform heavy-duty data processing and combining

**Garmin Health Metrics Flow**:
1. **Preprocessor**: Prepares data for processing  
2. **Merger**: Combines data from multiple files
3. **Cleaner**: Removes temporary files after processing

**BBI (Beat-to-Beat Interval) Flow**: 
1. **Preprocessor**: Prepares BBI data for analysis
2. **Merger**: Combines BBI data
3. **Cleaner**: Removes temporary files after processing

### 🎯 Workflow Orchestrator (Glue Workflows)
**What they do**: Ensure jobs run in the correct order

- Makes sure the preprocessor runs before the merger
- Makes sure the merger runs before the cleaner 
- Automatically starts the next job when the previous one finishes

### 📡 Event System (EventBridge)
**What they do**: Send notifications when important events happen

- Notifies the system when a batch of files is complete
- Triggers the data processing workflows automatically

---

## 🚀 Getting Started (For Developers) 

### Prerequisites
- Python 3.13 or higher
- AWS Account with appropriate permissions
- AWS CDK installed 

### Initial Setup

1. **Navigate to the infrastructure folder**:
   ```bash
   cd infra
   ```

2. **Create a virtual environment** (isolated Python environment):
   ```bash  
   python3 -m venv .venv
   ```

3. **Activate the virtual environment**:
   - **Mac/Linux**: `source .venv/bin/activate`
   - **Windows**: `.venv\Scripts\activate.bat`

4. **Install required packages**:
   ```bash
   pip install -r requirements.txt
   ```

### Deploying the System

#### Multi-Environment Support

This system supports deploying to multiple environments. All resources are automatically named with environment-specific suffixes to ensure complete isolation between environments.

**Available Environments:**
- `dev` - Development environment (default)
- `sandbox` - Sandbox environment for testing
- `sandox` - Sandox environment
- `uat` - User Acceptance Testing environment
- `qe` - Quality Engineering environment
- `staging` - Staging environment
- `prod` - Production environment

#### Deployment Steps

1. **First-time setup** (only needed once per AWS account/region):
   ```bash
   cdk bootstrap
   ```

2. **Preview changes** (see what will be created/updated):
   ```bash  
   cdk diff --context env=dev
   ```

3. **Deploy to a specific environment**:
   ```bash
   # Deploy to development (default)
   cdk deploy
   # or explicitly:
   cdk deploy --context env=dev
   
   # Deploy to staging
   cdk deploy --context env=staging
   
   # Deploy to production
   cdk deploy --context env=prod
   
   # Deploy to UAT
   cdk deploy --context env=uat
   ```

4. **Deploy with specific AWS account and region**:
   ```bash
   cdk deploy --context env=staging --context account=123456789012 --context region=us-west-2
   ```

5. **Remove the system** (when you want to delete everything):
   ```bash
   cdk destroy --context env=dev
   ```

#### Environment-Specific Resource Naming

All resources are automatically named with the environment suffix to ensure complete isolation:

- **S3 Buckets**: `albanyhealthsource-s3bucket-{env}`, `albanyhealthprocessed-s3bucket-{env}`, etc.
- **Lambda Functions**: `albanyHealth-main-router-lambda-function-{env}`, etc.
- **SQS Queues**: `AlbanyHealthMain-SQSQueue-{ENV}`, `AlbanyHealthMain-DLQ-{ENV}`, etc.
- **Glue Jobs**: `AlbanyHealthGarmin-Data-Preprocessor-Glue-Job-{env}`, etc.
- **Glue Workflows**: `AlbanyHealthGarminHealthMetricsWorkflow-{env}`, etc.
- **EventBridge Rules**: `trigger-merge-patient-health-metrics-{env}`, etc.
- **IAM Roles**: `AlbanyHealthGlueJobRole-{env}`, `AlbanyHealthS3ToSQSRole-{env}`, etc.
- **Stack Name**: `AlbanyHealthManagementStack-{env}`

#### Resource Tags

All resources are automatically tagged with:
- `Environment`: The environment name (dev/sandbox/uat/qe/staging/prod)
- `Project`: AlbanyHealthManagement

This allows you to:
- Filter resources by environment in AWS Console
- Apply environment-specific policies
- Track costs per environment
- Deploy multiple environments in the same AWS account

> **Note**: For detailed deployment instructions and troubleshooting, see `infra/DEPLOYMENT.md`

### Development Commands

- `cdk synth --context env=dev` - Generate the deployment template for a specific environment
- `cdk ls` - List all stacks in the project
- `cdk diff --context env=dev` - Preview changes before deploying
- `cdk watch --context env=dev` - Automatically redeploy when code changes
- `pytest tests` - Run automated tests

### Environment Configuration

Environment configurations are managed in `infra/albany_health_management/config.py`. Each environment can have:
- Custom AWS account and region settings
- Environment-specific resource configurations
- Custom feature flags or settings

To add a new environment or modify existing ones, edit the `ENVIRONMENTS` dictionary in `config.py`.

---

## 📁 Project Structure

```
Albany-Health-Management/
│
├── 📂 infra/                         # Infrastructure code (how the system is built)
│   ├── app.py                        # Main application entry point
│   ├── cdk.json                      # CDK configuration
│   ├── DEPLOYMENT.md                 # Multi-environment deployment guide
│   └── albany_health_management/     
│       ├── config.py                 # Environment configuration
│       ├── albany_health_management_stack.py  # Main system definition
│       └── constructs/               # Individual components
│           ├── s3_buckets.py         # Storage bucket definitions 
│           ├── sqs_queues.py         # Message queue definitions
│           ├── lambda_functions.py   # Processing worker definitions
│           ├── glue_jobs.py          # Data processing job definitions
│           ├── glue_workflows.py     # Workflow orchestrator definitions
│           └── eventbridge_rules.py  # Event notification rules
│  
├── 📂 services/                      # Application code (what the system does)
│   ├── ingestion/                    # Data intake services
│   │   └── lambdas/                  # Processing workers   
│   │       ├── albanyHealth-main-router-lambda-function/         # Routes files to correct queue
│   │       ├── albanyHealth-heart-rate-lambda-function/          # Processes heart rate data
│   │       ├── albanyHealth-sleep-lambda-function/               # Processes sleep data
│   │       ├── albanyHealth-step-lambda-function/                # Processes step data  
│   │       ├── albanyHealth-other-metrics-lambda-function/       # Processes other health metrics
│   │       ├── albanyHealth-data-inactivity-checker-lambda-function/      # Monitors batch completion
│   │       ├── albanyHealth-activate-garmin-triggers-lambda-function/     # Activates Garmin workflow triggers
│   │       └── albanyHealth-activate-bbi-triggers-lambda-function/        # Activates BBI workflow triggers
│   │   
│   └── analytics/                    # Data processing services
│       └── glue_jobs/                # Heavy-duty data processing scripts  
│           ├── AlbanyHealth-Garmin-Health-metrics-Flow-Script/  # Health metrics processing
│           └── AlbanyHealth-BBI-Flow-Script/            # BBI data processing
│
├── 📂 docs/                           # Documentation and guides
├── 📂 tests/                          # Automated tests  
└── 📂 tools/                          # Helper scripts
```

--- 

## 🔍 Monitoring & Troubleshooting

### Where to Check System Status

1. **AWS CloudWatch Console**: 
   - View logs from all Lambda functions (log groups are environment-specific)
   - See error messages if something goes wrong
   - Monitor system performance
   - Filter by environment using tags

2. **AWS Glue Console**:
   - Check if data processing jobs are running (jobs are named with environment suffix)
   - See job execution history  
   - View job success/failure status
   - Workflows are named: `AlbanyHealthGarminHealthMetricsWorkflow-{env}`

3. **AWS S3 Console**:
   - Verify files are arriving in the source bucket (buckets are environment-specific)
   - Check if processed files are being created
   - Monitor storage usage
   - Bucket names: `albanyhealthsource-s3bucket-{env}`, etc.

4. **AWS CloudFormation Console**:
   - View stack status: `AlbanyHealthManagementStack-{env}`
   - Check resource creation status
   - View stack events and errors 

### Common Issues

**Problem**: Files are not being processed
- **Check**: Are files arriving in the correct environment's source S3 bucket? (e.g., `albanyhealthsource-s3bucket-{env}`)
- **Check**: Are there any error messages in CloudWatch logs for the specific environment?
- **Check**: Are the SQS queues empty or backed up? (Queue names include environment: `AlbanyHealthMain-SQSQueue-{ENV}`)
- **Check**: Are you looking at the correct environment's resources?

**Problem**: Data processing jobs are failing
- **Check**: CloudWatch logs for the specific Glue job (jobs are named with environment suffix)
- **Check**: Are there permission errors? 
- **Check**: Is the data format correct?
- **Check**: Verify you're checking the correct environment's Glue jobs

**Problem**: System is slow
- **Check**: How many files are waiting in SQS queues? (Use environment-specific queue names)
- **Check**: Are the Lambda functions running? (Function names include environment suffix)
- **Check**: Are there any error messages?
- **Check**: Verify you're monitoring the correct environment

**Problem**: Deployment fails with "ResourceExistenceCheck Failed"
- **Check**: S3 bucket names are globally unique - ensure bucket names don't already exist
- **Check**: Verify account and region are correctly configured
- **Check**: See `infra/DEPLOYMENT.md` for detailed troubleshooting steps

---

## ⚙️ System Configuration

### Current Settings

- **Lambda Capacity**: 10 concurrent executions per function (for faster processing) 
- **Glue Worker Type**: G.1X (4 vCPU, 16 GB memory per worker)
- **Permissions**: Least privilege IAM roles (for security best practices)
- **Log Retention**: 
  - Lambda function logs: Automatically managed by CDK (environment-specific log groups)
  - Explicit log groups: 7 days retention
  - All logs are environment-specific and isolated

### Multi-Environment Architecture

- **Complete Resource Isolation**: Each environment has its own set of resources with unique names
- **Environment-Specific Naming**: All resources include the environment suffix
- **Automatic Tagging**: All resources are tagged with environment and project name
- **Stack Isolation**: Each environment deploys as a separate CloudFormation stack
- **IAM Role Isolation**: Each environment has its own IAM roles for security

### Data Processing Requirements

The system expects the following files per patient batch:
- **Stress files**: 7 files 
- **Step files**: 7 files
- **Respiration files**: 7 files 
- **Pulse-ox files**: 7 files
- **Heart rate files**: 7 files 
- **Sleep-stage files**: 1 file (always)

Once all required files are received, the batch is automatically processed.

---

## 📞 Support & Questions

If you encounter issues or have questions:

1. Check the CloudWatch logs for error messages
2. Review the system status in AWS Console
3. Check this README for common troubleshooting steps
4. Contact the development team with specific error messages

---

## 🔐 Security Notes

- **Environment Isolation**: Each environment has separate IAM roles and resources
- **IAM Roles**: Environment-specific roles (e.g., `AlbanyHealthGlueJobRole-{env}`)
- **Data Storage**: All data is stored securely in AWS S3 buckets with environment-specific names
- **Access Control**: Access is controlled through AWS IAM roles per environment
- **Log Retention**: 
  - Lambda logs: Automatically managed, environment-specific
  - Explicit log groups: 7 days retention
- **Resource Tagging**: All resources are tagged for security and compliance tracking
- **Multi-Account Support**: Can deploy different environments to different AWS accounts for enhanced security


---

## 📝 Version Information

- **CDK Version**: 2.x
- **Python Version**: 3.13+
- **Glue Version**: 5.0  
- **Lambda Runtime**: Python 3.13

---