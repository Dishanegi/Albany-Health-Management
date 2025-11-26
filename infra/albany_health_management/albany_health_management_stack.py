from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_lambda_event_sources as lambda_event_sources,
    aws_events_targets as targets,
)
from constructs import Construct
from .constructs.s3_buckets import S3Buckets
from .constructs.sqs_queues import SQSQueues
from .constructs.lambda_functions import LambdaFunctions
from .constructs.glue_jobs import GlueJobs
from .constructs.glue_workflows import GlueWorkflows
from .constructs.eventbridge_rules import EventBridgeRules

class AlbanyHealthManagementStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 buckets
        s3_buckets = S3Buckets(self, "S3Buckets")

        # Create SQS queues
        sqs_queues = SQSQueues(self, "SQSQueues")

        # Create Lambda functions
        lambda_functions = LambdaFunctions(
            self,
            "LambdaFunctions",
            sqs_queues.main_queue,
            sqs_queues.heart_rate_queue,
            sqs_queues.others_queue,
            sqs_queues.sleep_queue,
            sqs_queues.step_queue,
            sqs_queues.processing_files_queue,
            s3_buckets.source_bucket,
            s3_buckets.processed_bucket,
        )
        

        # Create Glue jobs with S3 bucket access
        glue_jobs = GlueJobs(self, "GlueJobs", s3_buckets=s3_buckets)

        # Create Glue workflows
        glue_workflows = GlueWorkflows(
            self,
            "GlueWorkflows",
            glue_jobs.glue_jobs,
            lambda_functions.activate_garmin_triggers_lambda,
            lambda_functions.activate_bbi_triggers_lambda,
        )

        # Create EventBridge rules
        event_bridge_rules = EventBridgeRules(
            self,
            "EventBridgeRules",
            lambda_functions.data_inactivity_checker_function,
            glue_workflows,
        )        
        # Add EventBridge rule names as environment variables to the data inactivity checker Lambda
        lambda_functions.data_inactivity_checker_function.add_environment(
            "EVENTBRIDGE_COMPLETION_RULE_NAME", 
            event_bridge_rules.completion_rule_name
        )
        lambda_functions.data_inactivity_checker_function.add_environment(
            "EVENTBRIDGE_PROCESSING_RULE_NAME", 
            event_bridge_rules.processing_rule_name
        )
        lambda_functions.data_inactivity_checker_function.add_environment(
            "EVENTBRIDGE_BUS_NAME", 
            "default"
        )
        
        # Add expected file count environment variables (configurable, defaults to 7)
        # Sleep-stage is always 1 and not configurable
        lambda_functions.data_inactivity_checker_function.add_environment(
            "EXPECTED_STRESS_FILES", 
            "7"
        )
        lambda_functions.data_inactivity_checker_function.add_environment(
            "EXPECTED_STEP_FILES", 
            "7"
        )
        lambda_functions.data_inactivity_checker_function.add_environment(
            "EXPECTED_RESPIRATION_FILES", 
            "7"
        )
        lambda_functions.data_inactivity_checker_function.add_environment(
            "EXPECTED_PULSE_OX_FILES", 
            "7"
        )
        lambda_functions.data_inactivity_checker_function.add_environment(
            "EXPECTED_HEART_RATE_FILES", 
            "7"
        )

        # Create an IAM role for S3 to send messages to SQS
        s3_to_sqs_role = iam.Role(
            self,
            "S3ToSQSRole",
            assumed_by=iam.ServicePrincipal("s3.amazonaws.com"),
        )

        # Grant the IAM role permissions to send messages to the SQS queues
        sqs_queues.main_queue.grant_send_messages(s3_to_sqs_role)
        sqs_queues.processing_files_queue.grant_send_messages(s3_to_sqs_role)

        s3_buckets.source_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(sqs_queues.main_queue)
        )

        s3_buckets.processed_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(sqs_queues.processing_files_queue)
        )

        # Configure the main SQS queue to trigger the main router Lambda function
        lambda_functions.main_router_function.add_event_source(lambda_event_sources.SqsEventSource(sqs_queues.main_queue))

        # Configure the heart rate SQS queue to trigger the heart rate Lambda function
        lambda_functions.heart_rate_function.add_event_source(lambda_event_sources.SqsEventSource(sqs_queues.heart_rate_queue))

        # Configure the others SQS queue to trigger the other metrics Lambda function
        lambda_functions.other_metrics_function.add_event_source(lambda_event_sources.SqsEventSource(sqs_queues.others_queue))

        # Configure the sleep SQS queue to trigger the sleep Lambda function
        lambda_functions.sleep_function.add_event_source(lambda_event_sources.SqsEventSource(sqs_queues.sleep_queue))

        # Configure the step SQS queue to trigger the step Lambda function
        lambda_functions.step_function.add_event_source(lambda_event_sources.SqsEventSource(sqs_queues.step_queue))

        # Configure the processing SQS queue to trigger the inactivity checker Lambda function
        lambda_functions.data_inactivity_checker_function.add_event_source(lambda_event_sources.SqsEventSource(sqs_queues.processing_files_queue))