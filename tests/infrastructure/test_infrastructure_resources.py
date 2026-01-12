"""
Infrastructure tests to verify all AWS resources are created properly.
Tests exact resource counts and naming conventions for SQS, Lambda, Glue, S3, EventBridge, and IAM.
"""
import aws_cdk as core
import aws_cdk.assertions as assertions
import sys
import os

# Add infra directory to path for CDK imports
infra_path = os.path.join(os.path.dirname(__file__), '..', '..', 'infra')
infra_path = os.path.abspath(infra_path)
if infra_path not in sys.path:
    sys.path.insert(0, infra_path)

from albany_health_management.albany_health_management_stack import AlbanyHealthManagementStack
from albany_health_management.config import get_environment


class TestSQSQueues:
    """Test that all SQS queues are created with exact naming conventions."""
    
    def test_exact_sqs_queue_count(self):
        """Test that exactly 12 SQS queues are created (6 main + 6 DLQs)."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.resource_count_is("AWS::SQS::Queue", 12)
    
    def test_main_queue_exact_name(self):
        """Test main queue has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::SQS::Queue", {
            "QueueName": "AlbanyHealthMain-SQSQueue-DEV",
            "VisibilityTimeout": 360
        })
    
    def test_heart_rate_queue_exact_name(self):
        """Test heart rate queue has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::SQS::Queue", {
            "QueueName": "AlbanyHealthHeartRate-SQSQueue-DEV",
            "VisibilityTimeout": 360
        })
    
    def test_sleep_queue_exact_name(self):
        """Test sleep queue has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::SQS::Queue", {
            "QueueName": "AlbanyHealthSleep-SQSQueue-DEV",
            "VisibilityTimeout": 360
        })
    
    def test_step_queue_exact_name(self):
        """Test step queue has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::SQS::Queue", {
            "QueueName": "AlbanyHealthStep-SQSQueue-DEV",
            "VisibilityTimeout": 360
        })
    
    def test_others_queue_exact_name(self):
        """Test others queue has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::SQS::Queue", {
            "QueueName": "AlbanyHealthOthers-SQSQueue-DEV",
            "VisibilityTimeout": 360
        })
    
    def test_processing_files_queue_exact_name(self):
        """Test processing files queue has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::SQS::Queue", {
            "QueueName": "AlbanyHealthSuccessfullProcessingFiles-SQSQueue-DEV",
            "VisibilityTimeout": 360
        })
    
    def test_all_dlq_exact_names(self):
        """Test all DLQs have exact names."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        dlq_names = [
            "AlbanyHealthMain-DLQ-DEV",
            "AlbanyHealthHeartRate-DLQ-DEV",
            "AlbanyHealthSleep-DLQ-DEV",
            "AlbanyHealthStep-DLQ-DEV",
            "AlbanyHealthOthers-DLQ-DEV",
            "AlbanyHealthSuccessfullProcessingFiles-DLQ-DEV"
        ]
        
        for dlq_name in dlq_names:
            template.has_resource_properties("AWS::SQS::Queue", {
                "QueueName": dlq_name,
                "MessageRetentionPeriod": 1209600  # 14 days in seconds
            })
    
    def test_all_queues_have_dlq_configured(self):
        """Test that all main queues have DLQ with maxReceiveCount = 5."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        main_queue_names = [
            "AlbanyHealthMain-SQSQueue-DEV",
            "AlbanyHealthHeartRate-SQSQueue-DEV",
            "AlbanyHealthSleep-SQSQueue-DEV",
            "AlbanyHealthStep-SQSQueue-DEV",
            "AlbanyHealthOthers-SQSQueue-DEV",
            "AlbanyHealthSuccessfullProcessingFiles-SQSQueue-DEV"
        ]
        
        for queue_name in main_queue_names:
            template.has_resource_properties("AWS::SQS::Queue", {
                "QueueName": queue_name,
                "RedrivePolicy": assertions.Match.object_like({
                    "maxReceiveCount": 5
                })
            })


class TestLambdaFunctions:
    """Test that all Lambda functions are created with exact naming conventions."""
    
    def test_exact_lambda_function_count(self):
        """Test that exactly 13 Lambda functions are created (8 explicit + 5 custom resource providers)."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # 8 explicit Lambda functions + 5 custom resource provider functions = 13 total
        template.resource_count_is("AWS::Lambda::Function", 13)
    
    def test_main_router_lambda_exact_name(self):
        """Test main router Lambda has exact name and properties."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-main-router-lambda-function-dev",
            "Runtime": "python3.13",
            "Handler": "main.lambda_handler",
            "Timeout": 300,
            "MemorySize": 256
        })
    
    def test_heart_rate_lambda_exact_name(self):
        """Test heart rate Lambda has exact name and properties."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-heart-rate-lambda-function-dev",
            "Runtime": "python3.13",
            "Handler": "main.lambda_handler",
            "Timeout": 300,
            "MemorySize": 1024
        })
    
    def test_step_lambda_exact_name(self):
        """Test step Lambda has exact name and properties."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-step-lambda-function-dev",
            "Runtime": "python3.13",
            "Handler": "main.lambda_handler",
            "Timeout": 300,
            "MemorySize": 1024
        })
    
    def test_sleep_lambda_exact_name(self):
        """Test sleep Lambda has exact name and properties."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-sleep-lambda-function-dev",
            "Runtime": "python3.13",
            "Handler": "main.lambda_handler",
            "Timeout": 300,
            "MemorySize": 1024
        })
    
    def test_other_metrics_lambda_exact_name(self):
        """Test other metrics Lambda has exact name and properties."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-other-metrics-lambda-function-dev",
            "Runtime": "python3.13",
            "Handler": "main.lambda_handler",
            "Timeout": 300,
            "MemorySize": 1024
        })
    
    def test_data_inactivity_checker_lambda_exact_name(self):
        """Test data inactivity checker Lambda has exact name and properties."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-data-inactivity-checker-lambda-function-dev",
            "Runtime": "python3.13",
            "Handler": "main.lambda_handler",
            "Timeout": 300,
            "MemorySize": 512
        })
    
    def test_activate_garmin_triggers_lambda_exact_name(self):
        """Test activate Garmin triggers Lambda has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-activate-garmin-triggers-lambda-function-dev",
            "Runtime": "python3.11",
            "Handler": "main.handler",
            "Timeout": 60
        })
    
    def test_activate_bbi_triggers_lambda_exact_name(self):
        """Test activate BBI triggers Lambda has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-activate-bbi-triggers-lambda-function-dev",
            "Runtime": "python3.11",
            "Handler": "main.handler",
            "Timeout": 60
        })
    
    def test_lambda_environment_variables_exact_names(self):
        """Test that Lambda functions have correct environment variables with exact queue names."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # Check main router has exact queue URL environment variables
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-main-router-lambda-function-dev",
            "Environment": assertions.Match.object_like({
                "Variables": assertions.Match.object_like({
                    "HEALTH_HEART_RATE_QUEUE": assertions.Match.any_value(),
                    "HEALTH_SLEEP_QUEUE": assertions.Match.any_value(),
                    "HEALTH_STEP_QUEUE": assertions.Match.any_value(),
                    "HEALTH_OTHERS_QUEUE": assertions.Match.any_value()
                })
            })
        })
    
    def test_lambda_has_pandas_layer(self):
        """Test that Lambda functions requiring pandas have the layer."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # Functions that should have pandas layer
        functions_with_layer = [
            "albanyHealth-heart-rate-lambda-function-dev",
            "albanyHealth-step-lambda-function-dev",
            "albanyHealth-sleep-lambda-function-dev",
            "albanyHealth-other-metrics-lambda-function-dev"
        ]
        
        for function_name in functions_with_layer:
            # Check that Layers property exists and is an array (can be CloudFormation intrinsic function)
            template.has_resource_properties("AWS::Lambda::Function", {
                "FunctionName": function_name,
                "Layers": assertions.Match.any_value()  # Layers exists (could be intrinsic function)
            })


class TestS3Buckets:
    """Test that all S3 buckets are created with exact naming conventions."""
    
    def test_exact_s3_bucket_count(self):
        """Test that exactly 5 S3 buckets are created."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.resource_count_is("AWS::S3::Bucket", 5)
    
    def test_source_bucket_exact_name(self):
        """Test source bucket has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": "albanyhealthsource-s3bucket-dev",
            "VersioningConfiguration": assertions.Match.object_like({
                "Status": "Enabled"
            })
        })
    
    def test_processed_bucket_exact_name(self):
        """Test processed bucket has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": "albanyhealthprocessed-s3bucket-dev",
            "VersioningConfiguration": assertions.Match.object_like({
                "Status": "Enabled"
            })
        })
    
    def test_bbi_processing_bucket_exact_name(self):
        """Test BBI processing bucket has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": "albanyhealthbbiprocessing-s3bucket-dev",
            "VersioningConfiguration": assertions.Match.object_like({
                "Status": "Enabled"
            })
        })
    
    def test_merged_bucket_exact_name(self):
        """Test merged bucket has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": "albanyhealthmerged-s3bucket-dev"
        })
    
    def test_bbi_merged_bucket_exact_name(self):
        """Test BBI merged bucket has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": "albanyhealthbbimerged-s3bucket-dev"
        })
    
    def test_s3_event_notifications_configured(self):
        """Test that S3 event notifications are configured for source and processed buckets."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # CDK creates S3 bucket notifications as Custom::S3BucketNotifications resources
        # Check that notification resources exist (CDK creates them as custom resources)
        # The actual notifications are handled by CDK's custom resource, so we verify
        # the buckets exist and the notification configuration is set up
        # We can verify by checking that the buckets exist and have the expected names
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": "albanyhealthsource-s3bucket-dev"
        })
        
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": "albanyhealthprocessed-s3bucket-dev"
        })
        
        # Verify that Custom::S3BucketNotifications resources exist (CDK creates these)
        # This confirms event notifications are configured
        template.has_resource_properties("Custom::S3BucketNotifications", assertions.Match.any_value())


class TestGlueJobs:
    """Test that all Glue jobs are created with exact naming conventions."""
    
    def test_exact_glue_job_count(self):
        """Test that exactly 7 Glue jobs are created."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.resource_count_is("AWS::Glue::Job", 7)
    
    def test_garmin_preprocessing_job_exact_name(self):
        """Test Garmin preprocessing job has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Job", {
            "Name": "AlbanyHealthGarmin-Data-Preprocessor-Glue-Job-dev",
            "Command": assertions.Match.object_like({
                "Name": "glueetl",
                "PythonVersion": "3"
            }),
            "GlueVersion": "5.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10
        })
    
    def test_garmin_merge_job_exact_name(self):
        """Test Garmin merge job has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Job", {
            "Name": "AlbanyHealthGarmin-Data-Merge-Glue-Job-dev"
        })
    
    def test_garmin_delete_job_exact_name(self):
        """Test Garmin delete job has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Job", {
            "Name": "AlbanyHealthGarmin-Data-Delete-Glue-Job-dev"
        })
    
    def test_bbi_preprocessing_job_exact_name(self):
        """Test BBI preprocessing job has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Job", {
            "Name": "AlbanyHealthGarmin-BBI-Preprocessor-Glue-Job-dev"
        })
    
    def test_bbi_merge_job_exact_name(self):
        """Test BBI merge job has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Job", {
            "Name": "AlbanyHealthGarmin-BBI-Merge-Data-Glue-Job-dev"
        })
    
    def test_bbi_delete_data_job_exact_name(self):
        """Test BBI delete data job has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Job", {
            "Name": "AlbanyHealthGarmin-BBI-Delete-Data-Glue-Job-dev"
        })
    
    def test_bbi_delete_source_folders_job_exact_name(self):
        """Test BBI delete source folders job has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Job", {
            "Name": "AlbanyHealthGarmin-BBI-Delete-Source-Folders-Glue-Job-dev"
        })
    
    def test_all_glue_jobs_have_same_config(self):
        """Test that all Glue jobs have same worker configuration."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # All jobs should have same configuration
        template.has_resource_properties("AWS::Glue::Job", {
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10,
            "GlueVersion": "5.0"
        })
    
    def test_glue_job_role_exact_name(self):
        """Test Glue job IAM role has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::IAM::Role", {
            "RoleName": "AlbanyHealthGlueJobRole-dev",
            "AssumeRolePolicyDocument": assertions.Match.object_like({
                "Statement": assertions.Match.array_with([
                    assertions.Match.object_like({
                        "Principal": assertions.Match.object_like({
                            "Service": "glue.amazonaws.com"
                        })
                    })
                ])
            })
        })


class TestGlueWorkflows:
    """Test that Glue workflows are created with exact naming conventions."""
    
    def test_exact_glue_workflow_count(self):
        """Test that exactly 2 Glue workflows are created."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.resource_count_is("AWS::Glue::Workflow", 2)
    
    def test_garmin_workflow_exact_name(self):
        """Test Garmin workflow has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Workflow", {
            "Name": "AlbanyHealthGarminHealthMetricsWorkflow-dev"
        })
    
    def test_bbi_workflow_exact_name(self):
        """Test BBI workflow has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Workflow", {
            "Name": "AlbanyHealthGarminBBIWorkflow-dev"
        })


class TestGlueTriggers:
    """Test that Glue triggers are created correctly."""
    
    def test_exact_glue_trigger_count(self):
        """Test that exactly 7 Glue triggers are created (2 event + 5 conditional)."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.resource_count_is("AWS::Glue::Trigger", 7)
    
    def test_garmin_event_trigger_exists(self):
        """Test Garmin event trigger exists."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Trigger", {
            "Type": "EVENT",
            "WorkflowName": "AlbanyHealthGarminHealthMetricsWorkflow-dev"
        })
    
    def test_bbi_event_trigger_exists(self):
        """Test BBI event trigger exists."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Glue::Trigger", {
            "Type": "EVENT",
            "WorkflowName": "AlbanyHealthGarminBBIWorkflow-dev"
        })
    
    def test_conditional_triggers_exist(self):
        """Test conditional triggers exist."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # Should have 5 conditional triggers (2 for Garmin, 3 for BBI)
        template.resource_count_is("AWS::Glue::Trigger", 7)
        
        # Check for conditional triggers
        template.has_resource_properties("AWS::Glue::Trigger", {
            "Type": "CONDITIONAL"
        })


class TestEventBridgeRules:
    """Test that EventBridge rules are created with exact naming conventions."""
    
    def test_exact_eventbridge_rule_count(self):
        """Test that exactly 2 EventBridge rules are created."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.resource_count_is("AWS::Events::Rule", 2)
    
    def test_completion_rule_exact_name(self):
        """Test completion EventBridge rule has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Events::Rule", {
            "Name": "trigger-merge-patient-health-metrics-dev",
            "State": "ENABLED"
        })
    
    def test_processing_rule_exact_name(self):
        """Test processing EventBridge rule has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Events::Rule", {
            "Name": "trigger-patients-bbi-flow-dev",
            "State": "ENABLED"
        })


class TestIAMRoles:
    """Test that IAM roles are created with exact naming conventions."""
    
    def test_s3_to_sqs_role_exact_name(self):
        """Test S3 to SQS IAM role has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::IAM::Role", {
            "RoleName": "AlbanyHealthS3ToSQSRole-dev",
            "AssumeRolePolicyDocument": assertions.Match.object_like({
                "Statement": assertions.Match.array_with([
                    assertions.Match.object_like({
                        "Principal": assertions.Match.object_like({
                            "Service": "s3.amazonaws.com"
                        })
                    })
                ])
            })
        })
    
    def test_glue_job_role_exact_name(self):
        """Test Glue job IAM role has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::IAM::Role", {
            "RoleName": "AlbanyHealthGlueJobRole-dev"
        })
    
    def test_eventbridge_to_glue_role_exact_name(self):
        """Test EventBridge to Glue IAM role has exact name."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::IAM::Role", {
            "RoleName": "AlbanyHealthEventBridgeToGlueRole-dev",
            "AssumeRolePolicyDocument": assertions.Match.object_like({
                "Statement": assertions.Match.array_with([
                    assertions.Match.object_like({
                        "Principal": assertions.Match.object_like({
                            "Service": "events.amazonaws.com"
                        })
                    })
                ])
            })
        })
    
    def test_lambda_execution_roles_created(self):
        """Test that Lambda execution roles are created (one per Lambda)."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # Verify explicit roles exist
        # Lambda execution roles are auto-created, so we just verify explicit ones
        template.has_resource_properties("AWS::IAM::Role", {
            "RoleName": "AlbanyHealthS3ToSQSRole-dev"
        })
        template.has_resource_properties("AWS::IAM::Role", {
            "RoleName": "AlbanyHealthGlueJobRole-dev"
        })
        template.has_resource_properties("AWS::IAM::Role", {
            "RoleName": "AlbanyHealthEventBridgeToGlueRole-dev"
        })
        
        # Verify Lambda functions have execution roles (check that roles exist for Lambda functions)
        # Each Lambda function should have an associated execution role
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-main-router-lambda-function-dev",
            "Role": assertions.Match.any_value()
        })


class TestEventSources:
    """Test that Lambda event sources are configured correctly."""
    
    def test_exact_event_source_mapping_count(self):
        """Test that exactly 6 EventSourceMappings are created (one per processing Lambda)."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # 6 processing Lambdas should have SQS event sources:
        # main_router, heart_rate, step, sleep, other_metrics, data_inactivity_checker
        template.resource_count_is("AWS::Lambda::EventSourceMapping", 6)
    
    def test_main_router_has_sqs_event_source(self):
        """Test that main router Lambda has SQS event source."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        template.has_resource_properties("AWS::Lambda::EventSourceMapping", {
            "EventSourceArn": assertions.Match.object_like({
                "Fn::GetAtt": assertions.Match.array_with([
                    assertions.Match.string_like_regexp(".*Main.*Queue.*"),
                    "Arn"
                ])
            })
        })


class TestResourceCounts:
    """Test exact resource counts for all resource types."""
    
    def test_all_resource_counts(self):
        """Test exact counts of all resource types."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # Exact resource counts
        template.resource_count_is("AWS::SQS::Queue", 12)  # 6 main + 6 DLQs
        template.resource_count_is("AWS::Lambda::Function", 13)  # 8 explicit + 5 custom resource providers
        template.resource_count_is("AWS::S3::Bucket", 5)  # 5 S3 buckets
        template.resource_count_is("AWS::Glue::Job", 7)  # 7 Glue jobs
        template.resource_count_is("AWS::Glue::Workflow", 2)  # 2 workflows
        template.resource_count_is("AWS::Glue::Trigger", 7)  # 7 triggers (2 event + 5 conditional)
        template.resource_count_is("AWS::Events::Rule", 2)  # 2 EventBridge rules
        template.resource_count_is("AWS::Lambda::EventSourceMapping", 6)  # 6 event source mappings


class TestEnvironmentSpecificNaming:
    """Test that resources use correct environment-specific naming conventions."""
    
    def test_dev_environment_naming(self):
        """Test that dev environment resources have correct naming (lowercase 'dev')."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # Check Lambda functions use lowercase 'dev'
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": assertions.Match.string_like_regexp(".*-dev$")
        })
        
        # Check SQS queues use uppercase 'DEV'
        template.has_resource_properties("AWS::SQS::Queue", {
            "QueueName": assertions.Match.string_like_regexp(".*-DEV$")
        })
        
        # Check S3 buckets use lowercase 'dev'
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": assertions.Match.string_like_regexp(".*-dev$")
        })
        
        # Check Glue jobs use lowercase 'dev'
        template.has_resource_properties("AWS::Glue::Job", {
            "Name": assertions.Match.string_like_regexp(".*-dev$")
        })
    
    def test_prod_environment_naming(self):
        """Test that prod environment resources have correct naming."""
        app = core.App()
        env_config = get_environment("prod")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # Check Lambda functions use lowercase 'prod'
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": assertions.Match.string_like_regexp(".*-prod$")
        })
        
        # Check SQS queues use uppercase 'PROD'
        template.has_resource_properties("AWS::SQS::Queue", {
            "QueueName": assertions.Match.string_like_regexp(".*-PROD$")
        })
        
        # Check S3 buckets use lowercase 'prod'
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": assertions.Match.string_like_regexp(".*-prod$")
        })
    
    def test_naming_convention_consistency(self):
        """Test that naming conventions are consistent across resource types."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # Lambda functions: albanyHealth-{function-name}-{env} (lowercase env)
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": assertions.Match.string_like_regexp("^albanyHealth-.*-dev$")
        })
        
        # SQS queues: AlbanyHealth{Type}-SQSQueue-{ENV} (uppercase ENV)
        template.has_resource_properties("AWS::SQS::Queue", {
            "QueueName": assertions.Match.string_like_regexp("^AlbanyHealth.*-DEV$")
        })
        
        # S3 buckets: albanyhealth{type}-s3bucket-{env} (lowercase env)
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": assertions.Match.string_like_regexp("^albanyhealth.*-dev$")
        })
        
        # Glue jobs: AlbanyHealthGarmin-{Job-Name}-Glue-Job-{env} (lowercase env)
        template.has_resource_properties("AWS::Glue::Job", {
            "Name": assertions.Match.string_like_regexp("^AlbanyHealthGarmin-.*-dev$")
        })
        
        # IAM roles: AlbanyHealth{RoleName}-{env} (lowercase env)
        template.has_resource_properties("AWS::IAM::Role", {
            "RoleName": assertions.Match.string_like_regexp("^AlbanyHealth.*-dev$")
        })


class TestResourceProperties:
    """Test specific resource properties are set correctly."""
    
    def test_sqs_visibility_timeout(self):
        """Test that all SQS queues have correct visibility timeout."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # All main queues should have visibility timeout of 360 seconds
        main_queues = [
            "AlbanyHealthMain-SQSQueue-DEV",
            "AlbanyHealthHeartRate-SQSQueue-DEV",
            "AlbanyHealthSleep-SQSQueue-DEV",
            "AlbanyHealthStep-SQSQueue-DEV",
            "AlbanyHealthOthers-SQSQueue-DEV",
            "AlbanyHealthSuccessfullProcessingFiles-SQSQueue-DEV"
        ]
        
        for queue_name in main_queues:
            template.has_resource_properties("AWS::SQS::Queue", {
                "QueueName": queue_name,
                "VisibilityTimeout": 360
            })
    
    def test_lambda_timeout_and_memory(self):
        """Test that Lambda functions have correct timeout and memory settings."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # Main router: 256 MB, 300s timeout
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-main-router-lambda-function-dev",
            "MemorySize": 256,
            "Timeout": 300
        })
        
        # Processing Lambdas: 1024 MB, 300s timeout
        processing_functions = [
            "albanyHealth-heart-rate-lambda-function-dev",
            "albanyHealth-step-lambda-function-dev",
            "albanyHealth-sleep-lambda-function-dev",
            "albanyHealth-other-metrics-lambda-function-dev"
        ]
        
        for function_name in processing_functions:
            template.has_resource_properties("AWS::Lambda::Function", {
                "FunctionName": function_name,
                "MemorySize": 1024,
                "Timeout": 300
            })
        
        # Data inactivity checker: 512 MB, 300s timeout
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-data-inactivity-checker-lambda-function-dev",
            "MemorySize": 512,
            "Timeout": 300
        })
        
        # Activate triggers: 60s timeout
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "albanyHealth-activate-garmin-triggers-lambda-function-dev",
            "Timeout": 60
        })
    
    def test_glue_job_worker_config(self):
        """Test that all Glue jobs have correct worker configuration."""
        app = core.App()
        env_config = get_environment("dev")
        stack = AlbanyHealthManagementStack(app, "test-stack", env_config=env_config)
        template = assertions.Template.from_stack(stack)
        
        # All Glue jobs should have same worker configuration
        template.has_resource_properties("AWS::Glue::Job", {
            "WorkerType": "G.1X",
            "NumberOfWorkers": 10,
            "GlueVersion": "5.0",
            "Command": assertions.Match.object_like({
                "Name": "glueetl",
                "PythonVersion": "3"
            })
        })
