# Albany Health Management Platform

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

1. **Data Arrival**: Health data files are uploaded to a storage location (S3 bucket)
2. **File Detection**: The system automatically detects when new files arrive
3. **File Routing**: Files are sorted into different queues based on their type:
   - Heart Rate data
   - Sleep data
   - Step data
   - Other health metrics (stress, respiration, pulse-ox)
4. **Data Processing**: Each file type is processed by specialized workers that:
   - Clean the data
   - Format it correctly
   - Add patient identification
5. **Batch Completion Check**: The system waits until all required files for a patient are received
6. **Data Merging**: Once complete, the data is combined and organized
7. **Final Storage**: Processed data is stored in organized locations for analysis

---

## 🏗️ System Components (What Each Part Does)

### 📦 Storage Buckets (S3)
**What they do**: Store all the health data files at different stages

- **Source Bucket**: Where new data files first arrive
- **Processed Bucket**: Where cleaned and organized data is stored
- **Merged Bucket**: Where final combined data is stored
- **BBI Processing Bucket**: Special storage for BBI (Beat-to-Beat Interval) data
- **BBI Merged Bucket**: Final storage for processed BBI data

### 📨 Message Queues (SQS)
**What they do**: Act like a waiting line for files to be processed

- Files wait in queues until a worker is ready to process them
- Different queues for different types of data (heart rate, sleep, steps, etc.)
- Ensures files are processed in order and nothing gets lost

### ⚙️ Processing Workers (Lambda Functions)
**What they do**: The "workers" that process the data files

- **Main Router**: Decides which queue each file should go to
- **Heart Rate Processor**: Processes heart rate data
- **Sleep Processor**: Processes sleep data
- **Step Processor**: Processes step data
- **Other Metrics Processor**: Processes stress, respiration, and pulse-ox data
- **Batch Checker**: Monitors when all files for a patient are received
- **Garmin Triggers Activator**: Automatically activates Glue workflow triggers for Garmin data processing
- **BBI Triggers Activator**: Automatically activates Glue workflow triggers for BBI data processing

### 🔄 Data Processing Jobs (Glue Jobs)
**What they do**: Heavy-duty data processing and combining

**Garmin Health Metrics Flow**:
1. **Preprocessor**: Prepares data for processing
2. **Merger**: Combines data from multiple files
3. **Cleaner**: Removes temporary files after processing

**BBI (Beat-to-Beat Interval) Flow**:
1. **Preprocessor**: Prepares BBI data for analysis
2. **Merger**: Combines BBI data
3. **Cleaner**: Removes temporary files after processing

### 🎯 Workflow Orchestrator (Glue Workflows)
**What they do**: Makes sure jobs run in the correct order

- Ensures the preprocessor runs before the merger
- Ensures the merger runs before the cleaner
- Automatically starts the next job when the previous one finishes

### 📡 Event System (EventBridge)
**What they do**: Sends notifications when important events happen

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

1. **First-time setup** (only needed once per AWS account/region):
   ```bash
cdk bootstrap
   ```

2. **Preview changes** (see what will be created/updated):
   ```bash
cdk diff
   ```

3. **Deploy the system**:
   ```bash
cdk deploy
   ```

4. **Remove the system** (when you want to delete everything):
   ```bash
cdk destroy
   ```

### Development Commands

- `cdk synth` - Generate the deployment template (without deploying)
- `cdk ls` - List all stacks in the project
- `cdk watch` - Automatically redeploy when code changes
- `pytest tests` - Run automated tests

---

## 📁 Project Structure

```
Albany-Health-Management/
│
├── 📂 infra/                          # Infrastructure code (how the system is built)
│   ├── app.py                         # Main application entry point
│   ├── cdk.json                       # CDK configuration
│   └── albany_health_management/
│       ├── albany_health_management_stack.py  # Main system definition
│       └── constructs/                # Individual components
│           ├── s3_buckets.py         # Storage bucket definitions
│           ├── sqs_queues.py         # Message queue definitions
│           ├── lambda_functions.py   # Processing worker definitions
│           ├── glue_jobs.py          # Data processing job definitions
│           ├── glue_workflows.py     # Workflow orchestrator definitions
│           └── eventbridge_rules.py  # Event notification rules
│
├── 📂 services/                       # Application code (what the system does)
│   ├── ingestion/                    # Data intake services
│   │   └── lambdas/                  # Processing workers
│   │       ├── albanyHealth-main-router-lambda-function/          # Routes files to correct queue
│   │       ├── albanyHealth-heart-rate-lambda-function/           # Processes heart rate data
│   │       ├── albanyHealth-sleep-lambda-function/                # Processes sleep data
│   │       ├── albanyHealth-step-lambda-function/                 # Processes step data
│   │       ├── albanyHealth-other-metrics-lambda-function/        # Processes other health metrics
│   │       ├── albanyHealth-data-inactivity-checker-lambda-function/       # Monitors batch completion
│   │       ├── albanyHealth-activate-garmin-triggers-lambda-function/      # Activates Garmin workflow triggers
│   │       └── albanyHealth-activate-bbi-triggers-lambda-function/         # Activates BBI workflow triggers
│   │
│   └── analytics/                    # Data processing services
│       └── glue_jobs/               # Heavy-duty data processing scripts
│           ├── AlbanyHealth-Garmin-Health-metrics-Flow-Script/  # Health metrics processing
│           └── AlbanyHealth-BBI-Flow-Script/             # BBI data processing
│
├── 📂 docs/                          # Documentation and guides
├── 📂 tests/                         # Automated tests
└── 📂 tools/                         # Helper scripts
```

---

## 🔍 Monitoring & Troubleshooting

### Where to Check System Status

1. **AWS CloudWatch Console**:
   - View logs from all processing workers
   - See error messages if something goes wrong
   - Monitor system performance

2. **AWS Glue Console**:
   - Check if data processing jobs are running
   - See job execution history
   - View job success/failure status

3. **AWS S3 Console**:
   - Verify files are arriving in the source bucket
   - Check if processed files are being created
   - Monitor storage usage

### Common Issues

**Problem**: Files are not being processed
- **Check**: Are files arriving in the source bucket?
- **Check**: Are there any error messages in CloudWatch logs?
- **Check**: Are the message queues empty or backed up?

**Problem**: Data processing jobs are failing
- **Check**: CloudWatch logs for the specific Glue job
- **Check**: Are there permission errors?
- **Check**: Is the data format correct?

**Problem**: System is slow
- **Check**: How many files are waiting in queues?
- **Check**: Are the processing workers running?
- **Check**: Are there any error messages?

---

## ⚙️ System Configuration

### Current Settings

- **Worker Capacity**: 10 workers per Glue job (for faster processing)
- **Worker Type**: G.1X (4 vCPU, 16 GB memory per worker)
- **Permissions**: Full admin access (for maximum flexibility)
- **Log Retention**: 7 days (logs are kept for 1 week)

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

- The system has full admin access for maximum flexibility
- All data is stored securely in AWS S3 buckets
- Access is controlled through AWS IAM roles
- Logs are retained for 7 days for troubleshooting

---

## 📝 Version Information

- **CDK Version**: Latest
- **Python Version**: 3.13+
- **Glue Version**: 5.0
- **Lambda Runtime**: Python 3.13

---

*Last Updated: November 2025*
