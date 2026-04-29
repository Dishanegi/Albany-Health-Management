import os
from pathlib import Path
from aws_cdk import (
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
    aws_iam as iam,
    aws_logs as logs,
    Duration,
    Stack,
)
from constructs import Construct
from ..config import EnvironmentConfig

# Define the AWS-provided account ID for Lambda layers
AWS_LAYER_ACCOUNT_ID = "336392948345"

# Define the AWS-provided pandas layer 
PANDAS_LAYER = "AWSSDKPandas-Python313"

# Define the AWS-provided pandas layer version
PANDAS_LAYER_VERSION = "1"

class LambdaFunctions(Construct):
    def __init__(self, scope: Construct, construct_id: str,
                 main_queue, heart_rate_queue, others_queue,
                 sleep_queue, step_queue,
                 processing_files_queue,
                 survey_data_file_queue,
                 survey_processing_files_queue,
                 source_bucket,
                 processed_bucket,
                 survey_data_processing_bucket, survey_data_merged_bucket,
                 batch_tracking_table: dynamodb.Table = None,
                 survey_batch_tracking_table: dynamodb.Table = None,
                 environment: EnvironmentConfig = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # Default to dev environment if not provided
        if environment is None:
            from ..config import get_environment
            environment = get_environment("dev")
        
        env_suffix = environment.name.lower()
        
        # Get AWS region from stack
        stack = Stack.of(self)
        aws_region = stack.region or "us-east-1"

        # Define the Pandas layer ARN using the account ID and region
        pandas_layer_arn = f"arn:aws:lambda:{aws_region}:{AWS_LAYER_ACCOUNT_ID}:layer:{PANDAS_LAYER}:{PANDAS_LAYER_VERSION}"

        # Get absolute path to services directory (relative to project root)
        # This file is in: infra/albany_health_management/constructs/lambda_functions.py
        # Project root is: infra/../ (two levels up)
        current_file = Path(__file__).resolve()
        project_root = current_file.parent.parent.parent.parent
        services_base_path = str(project_root / "services" / "ingestion" / "lambdas")

        # ── Shared utilities Lambda Layer ─────────────────────────────────────────
        # Contains utils.py (extract_participant_id, format_iso_timestamp,
        # write_placeholder).  The directory must have a python/ subdirectory so
        # Lambda's Python runtime adds /opt/python to sys.path automatically.
        shared_layer_path = str(project_root / "services" / "ingestion" / "lambdas" / "shared")
        self.shared_utils_layer = lambda_.LayerVersion(
            self,
            f"SharedUtilsLayer-{env_suffix.capitalize()}",
            code=lambda_.Code.from_asset(shared_layer_path),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
            description="Shared utilities: participant ID extraction, timestamp formatting, placeholder writes",
            removal_policy=RemovalPolicy.DESTROY,
        )

        main_router_log_group = logs.LogGroup(
            self,
            f"MainRouterLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-main-router-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.main_router_function = lambda_.Function(
            self,
            f"AlbanyHealthMainRouterLambdaFunction{env_suffix.capitalize()}",
            function_name=f"albanyHealth-main-router-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-main-router-lambda-function"),
            timeout=Duration.seconds(300),
            memory_size=256,
            log_group=main_router_log_group,
            environment={
                "HEALTH_HEART_RATE_QUEUE": heart_rate_queue.queue_url,
                "HEALTH_SLEEP_QUEUE": sleep_queue.queue_url,
                "HEALTH_STEP_QUEUE": step_queue.queue_url,
                "HEALTH_OTHERS_QUEUE": others_queue.queue_url,
                "HEALTH_SURVEY_DATA_QUEUE": survey_data_file_queue.queue_url,
            }
        )

        heart_rate_log_group = logs.LogGroup(
            self,
            f"HeartRateLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-heart-rate-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.heart_rate_function = lambda_.Function(
            self,
            f"AlbanyHealthHeartRateLambdaFunction{env_suffix.capitalize()}",
            function_name=f"albanyHealth-heart-rate-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-heart-rate-lambda-function"),
            timeout=Duration.seconds(300),
            memory_size=1024,
            log_group=heart_rate_log_group,
            layers=[
                self.shared_utils_layer,
                lambda_.LayerVersion.from_layer_version_arn(
                    self,
                    f"HeartRatePandasLayer-{env_suffix.capitalize()}",
                    pandas_layer_arn
                ),
            ],
            environment={
                "DESTINATION_BUCKET": processed_bucket.bucket_name,
            }
        )

        step_log_group = logs.LogGroup(
            self,
            f"StepLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-step-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.step_function = lambda_.Function(
            self,
            f"AlbanyHealthStepLambdaFunction{env_suffix.capitalize()}",
            function_name=f"albanyHealth-step-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-step-lambda-function"),
            timeout=Duration.seconds(300),
            memory_size=1024,
            log_group=step_log_group,
            layers=[
                self.shared_utils_layer,
                lambda_.LayerVersion.from_layer_version_arn(
                    self,
                    f"StepsPandasLayer-{env_suffix.capitalize()}",
                    pandas_layer_arn
                ),
            ],
            environment={
                "DESTINATION_BUCKET": processed_bucket.bucket_name,
            }
        )

        sleep_log_group = logs.LogGroup(
            self,
            f"SleepLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-sleep-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.sleep_function = lambda_.Function(
            self,
            f"AlbanyHealthSleepLambdaFunction{env_suffix.capitalize()}",
            function_name=f"albanyHealth-sleep-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-sleep-lambda-function"),
            timeout=Duration.seconds(300),
            memory_size=1024,
            log_group=sleep_log_group,
            layers=[
                self.shared_utils_layer,
                lambda_.LayerVersion.from_layer_version_arn(
                    self,
                    f"SleepPandasLayer-{env_suffix.capitalize()}",
                    pandas_layer_arn
                ),
            ],
            environment={
                "DESTINATION_BUCKET": processed_bucket.bucket_name,
            }
        )

        other_metrics_log_group = logs.LogGroup(
            self,
            f"OtherMetricsLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-other-metrics-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.other_metrics_function = lambda_.Function(
            self,
            f"AlbanyHealthOtherMetricsLambdaFunction{env_suffix.capitalize()}",
            function_name=f"albanyHealth-other-metrics-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-other-metrics-lambda-function"),
            timeout=Duration.seconds(300),
            memory_size=1024,
            log_group=other_metrics_log_group,
            layers=[
                self.shared_utils_layer,
                lambda_.LayerVersion.from_layer_version_arn(
                    self,
                    f"OtherMetricsPandasLayer-{env_suffix.capitalize()}",
                    pandas_layer_arn
                ),
            ],
            environment={
                "DESTINATION_BUCKET": processed_bucket.bucket_name,
            }
        )

        inactivity_checker_log_group = logs.LogGroup(
            self,
            f"InactivityCheckerLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-data-inactivity-checker-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.data_inactivity_checker_function = lambda_.Function(
            self,
            f"AlbanyHealthDataInactivityCheckerLambdaFunction{env_suffix.capitalize()}",
            function_name=f"albanyHealth-data-inactivity-checker-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-data-inactivity-checker-lambda-function"),
            timeout=Duration.seconds(300),
            memory_size=512,
            log_group=inactivity_checker_log_group,
            environment={
                "BATCH_TRACKING_TABLE": batch_tracking_table.table_name if batch_tracking_table else f"AlbanyHealthBatchTracking-{env_suffix}",
                "SOURCE_BUCKET": source_bucket.bucket_name,
                "EVENTBRIDGE_BUS_NAME": "default",
                "EVENTBRIDGE_COMPLETION_RULE_NAME": f"trigger-merge-patient-health-metrics-{env_suffix}",
                "EVENTBRIDGE_PROCESSING_RULE_NAME": f"trigger-patients-bbi-flow-{env_suffix}",
                "EVENTBRIDGE_GARMIN_DETAIL_TYPE": f"AlbanyHealthGarminHealthMetricsWorkflow-{env_suffix}",
                "EVENTBRIDGE_BBI_DETAIL_TYPE": f"AlbanyHealthGarminBBIWorkflow-{env_suffix}",
                "EVENTBRIDGE_SURVEY_RULE_NAME": f"trigger-survey-data-merged-files-{env_suffix}",
                "EVENTBRIDGE_SURVEY_DETAIL_TYPE": f"AlbanyHealthSurveyDataMergedFiles-{env_suffix}",
                "EVENTBRIDGE_SOURCE": "lambda",
            }
        )

        # Grant the inactivity checker read/write access to the DynamoDB batch tracking table
        if batch_tracking_table:
            batch_tracking_table.grant_read_write_data(self.data_inactivity_checker_function)

        # Grant the inactivity checker read access to the source bucket so it can
        # count how many files the patient uploaded per data type (dynamic expected count)
        source_bucket.grant_read(self.data_inactivity_checker_function)

        survey_normalise_log_group = logs.LogGroup(
            self,
            f"SurveyNormaliseLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-survey-data-normalise-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.survey_data_normalise_function = lambda_.Function(
            self,
            f"AlbanyHealthSurveyDataNormaliseLambdaFunction{env_suffix.capitalize()}",
            function_name=f"albanyHealth-survey-data-normalise-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-survey-data-normalise-lambda-function"),
            timeout=Duration.seconds(300),
            memory_size=1024,
            log_group=survey_normalise_log_group,
            layers=[
                self.shared_utils_layer,
                lambda_.LayerVersion.from_layer_version_arn(
                    self,
                    f"SurveyDataPandasLayer-{env_suffix.capitalize()}",
                    pandas_layer_arn
                ),
            ],
            environment={
                "DESTINATION_BUCKET": survey_data_processing_bucket.bucket_name,
            }
        )

        survey_merged_log_group = logs.LogGroup(
            self,
            f"SurveyMergedFilesLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-survey-data-merged-files-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.survey_data_merged_files_function = lambda_.Function(
            self,
            f"AlbanyHealthSurveyDataMergedFilesLambdaFunction{env_suffix.capitalize()}",
            function_name=f"albanyHealth-survey-data-merged-files-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-survey-data-merged-files-lambda-function"),
            timeout=Duration.seconds(300),
            memory_size=1024,
            log_group=survey_merged_log_group,
            layers=[lambda_.LayerVersion.from_layer_version_arn(
                self,
                f"SurveyDataMergedFilesPandasLayer-{env_suffix.capitalize()}",
                pandas_layer_arn
            )],
            environment={
                "DESTINATION_BUCKET": survey_data_merged_bucket.bucket_name,
                "SOURCE_BUCKET": survey_data_processing_bucket.bucket_name,
                "EVENTBRIDGE_DELETE_DETAIL_TYPE": f"AlbanyHealthSurveyDataDeleteProcessingFiles-{env_suffix}",
                "EVENTBRIDGE_BUS_NAME": "default",
            }
        )

        survey_delete_log_group = logs.LogGroup(
            self,
            f"SurveyDeleteFilesLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-survey-delete-processing-files-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.survey_data_delete_processing_files_function = lambda_.Function(
            self,
            f"AlbanyHealthSurveyDataDeleteProcessingFilesLambdaFunction{env_suffix.capitalize()}",
            function_name=f"albanyHealth-survey-delete-processing-files-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-survey-data-delete-processing-files-lambda-function"),
            timeout=Duration.seconds(300),
            memory_size=1024,
            log_group=survey_delete_log_group,
            environment={
                "SOURCE_BUCKET": survey_data_processing_bucket.bucket_name,
            }
        )

        survey_batch_checker_log_group = logs.LogGroup(
            self,
            f"SurveyBatchCheckerLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-survey-batch-checker-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.survey_batch_checker_function = lambda_.Function(
            self,
            f"AlbanyHealthSurveyBatchCheckerLambdaFunction{env_suffix.capitalize()}",
            function_name=f"albanyHealth-survey-batch-checker-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-survey-batch-checker-lambda-function"),
            timeout=Duration.seconds(300),
            memory_size=512,
            log_group=survey_batch_checker_log_group,
            environment={
                "SURVEY_BATCH_TRACKING_TABLE": survey_batch_tracking_table.table_name if survey_batch_tracking_table else f"AlbanyHealthSurveyBatchTracking-{env_suffix}",
                "SURVEY_PROCESSING_BUCKET": survey_data_processing_bucket.bucket_name,
                "EVENTBRIDGE_BUS_NAME": "default",
                "EVENTBRIDGE_SURVEY_RULE_NAME": f"trigger-survey-data-merged-files-{env_suffix}",
                "EVENTBRIDGE_SURVEY_DETAIL_TYPE": f"AlbanyHealthSurveyDataMergedFiles-{env_suffix}",
            }
        )

        if survey_batch_tracking_table:
            survey_batch_tracking_table.grant_read_write_data(self.survey_batch_checker_function)

        survey_data_processing_bucket.grant_read(self.survey_batch_checker_function)
        survey_processing_files_queue.grant_consume_messages(self.survey_batch_checker_function)

        # Grant the main router function permission to send messages to the heart rate, others, sleep, and step queues
        heart_rate_queue.grant_send_messages(self.main_router_function)
        others_queue.grant_send_messages(self.main_router_function)
        sleep_queue.grant_send_messages(self.main_router_function)
        step_queue.grant_send_messages(self.main_router_function)
        survey_data_file_queue.grant_send_messages(self.main_router_function)

        # Grant the main router function permission to consume messages from the main queue
        main_queue.grant_consume_messages(self.main_router_function)

        # Grant the heart rate function permission to consume messages from the heart rate queue
        heart_rate_queue.grant_consume_messages(self.heart_rate_function)

        # Grant the other metrics function permission to consume messages from the others queue
        others_queue.grant_consume_messages(self.other_metrics_function)

        # Grant the sleep function permission to consume messages from the sleep queue
        sleep_queue.grant_consume_messages(self.sleep_function)

        # Grant the step function permission to consume messages from the step queue
        step_queue.grant_consume_messages(self.step_function)

        # Grant the inactivity checker function permission to consume messages from the processing files queue
        processing_files_queue.grant_consume_messages(self.data_inactivity_checker_function)
        survey_data_file_queue.grant_consume_messages(self.survey_data_normalise_function)
        


        # Grant read access to the source_bucket for the heart rate, step, sleep, other and survey data functions
        source_bucket.grant_read(self.heart_rate_function)
        source_bucket.grant_read(self.step_function)
        source_bucket.grant_read(self.sleep_function)
        source_bucket.grant_read(self.other_metrics_function)
        source_bucket.grant_read(self.survey_data_normalise_function)
        

        # Grant write access to the processed_bucket for the heart rate, step, sleep, and other functions
        processed_bucket.grant_write(self.heart_rate_function)
        processed_bucket.grant_write(self.step_function)
        processed_bucket.grant_write(self.sleep_function)
        processed_bucket.grant_write(self.other_metrics_function)

        #Grant write access
        survey_data_processing_bucket.grant_write(self.survey_data_normalise_function)
        survey_data_processing_bucket.grant_read(self.survey_data_delete_processing_files_function)
        survey_data_processing_bucket.grant_delete(self.survey_data_delete_processing_files_function)
        survey_data_processing_bucket.grant_read(self.survey_data_merged_files_function)
        survey_data_merged_bucket.grant_write(self.survey_data_merged_files_function)
        survey_data_merged_bucket.grant_read(self.survey_data_merged_files_function)
        
        # Grant read and write access to the processed_bucket for the data inactivity checker function
        # It needs ListBucket permission to check for batch.json files and read/write them
        processed_bucket.grant_read(self.data_inactivity_checker_function)
        processed_bucket.grant_write(self.data_inactivity_checker_function)


        # Create Lambda functions to activate Glue CONDITIONAL triggers after deployment
        # These ensure triggers are ACTIVATED without manual intervention
        
        # Lambda function to activate Garmin workflow triggers
        # Explicitly create log group with DESTROY removal policy to ensure it's deleted on stack destroy
        activate_garmin_log_group = logs.LogGroup(
            self,
            f"ActivateGarminTriggersLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-activate-garmin-triggers-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.activate_garmin_triggers_lambda = lambda_.Function(
            self,
            "ActivateGarminTriggersLambda",
            function_name=f"albanyHealth-activate-garmin-triggers-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="main.handler",
            timeout=Duration.seconds(60),
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-activate-garmin-triggers-lambda-function"),
            log_group=activate_garmin_log_group,
            environment={
                "WORKFLOW_NAME": f"AlbanyHealthGarminHealthMetricsWorkflow-{env_suffix}",
                "ENVIRONMENT": env_suffix,
            },
        )
        
        # Grant permissions to activate triggers
        self.activate_garmin_triggers_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetTrigger",
                    "glue:GetTriggers",
                    "glue:StartTrigger",
                ],
                resources=["*"],  # Need to access all triggers to find the ones for this workflow
            )
        )
        
        # Lambda function to activate BBI workflow triggers
        # Explicitly create log group with DESTROY removal policy to ensure it's deleted on stack destroy
        activate_bbi_log_group = logs.LogGroup(
            self,
            f"ActivateBBITriggersLogGroup-{env_suffix.capitalize()}",
            log_group_name=f"/aws/lambda/albanyHealth-activate-bbi-triggers-lambda-function-{env_suffix}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        
        self.activate_bbi_triggers_lambda = lambda_.Function(
            self,
            "ActivateBBITriggersLambda",
            function_name=f"albanyHealth-activate-bbi-triggers-lambda-function-{env_suffix}",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="main.handler",
            timeout=Duration.seconds(60),
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-activate-bbi-triggers-lambda-function"),
            log_group=activate_bbi_log_group,
            environment={
                "WORKFLOW_NAME": f"AlbanyHealthGarminBBIWorkflow-{env_suffix}",
                "ENVIRONMENT": env_suffix,
            },
        )
        
        # Grant permissions to activate triggers
        self.activate_bbi_triggers_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetTrigger",
                    "glue:GetTriggers",
                    "glue:StartTrigger",
                ],
                resources=["*"],  # Need to access all triggers to find the ones for this workflow
            )
        )

        # Apply DESTROY removal policy to all Lambda functions so they are deleted on cdk destroy.
        # Log groups are handled by the explicit logs.LogGroup constructs above (each has
        # removal_policy=RemovalPolicy.DESTROY), so no extra log-group handling is needed here.
        for func in [
            self.main_router_function,
            self.heart_rate_function,
            self.step_function,
            self.sleep_function,
            self.other_metrics_function,
            self.data_inactivity_checker_function,
            self.activate_garmin_triggers_lambda,
            self.activate_bbi_triggers_lambda,
            self.survey_data_normalise_function,
            self.survey_data_merged_files_function,
            self.survey_data_delete_processing_files_function,
            self.survey_batch_checker_function,
        ]:
            func.apply_removal_policy(RemovalPolicy.DESTROY)