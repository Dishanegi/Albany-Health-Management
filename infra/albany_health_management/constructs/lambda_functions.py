from aws_cdk import (
    
    aws_lambda as lambda_,
    RemovalPolicy,
    aws_iam as iam,
    Duration

)
from constructs import Construct

# Define the AWS-provided account ID for Lambda layers
AWS_LAYER_ACCOUNT_ID = "336392948345"

# Define the AWS region
AWS_REGION = "us-east-1"

# Define the AWS-provided pandas layer 
PANDAS_LAYER = "AWSSDKPandas-Python313"

# Define the AWS-provided pandas layer version
PANDAS_LAYER_VERSION = "1"

class LambdaFunctions(Construct):
    def __init__(self, scope: Construct, construct_id: str, 
                 main_queue, heart_rate_queue, others_queue, 
                 sleep_queue, step_queue,
                 processing_files_queue,source_bucket, 
                 processed_bucket, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define the Pandas layer ARN using the account ID and region
        pandas_layer_arn = f"arn:aws:lambda:{AWS_REGION}:{AWS_LAYER_ACCOUNT_ID}:layer:{PANDAS_LAYER}:{PANDAS_LAYER_VERSION}"

        services_base_path = "../services/ingestion/lambdas"

        self.main_router_function = lambda_.Function(
            self,
            "AlbanyHealthMainRouterLambdaFunctionDev",
            function_name="albanyHealth-main-router-lambda-function-dev",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-main-router-lambda-function"),
            timeout=Duration.seconds(300),  # 5 minutes
            memory_size=256,  # 256 MB - lightweight routing function
            environment={
                "HEALTH_HEART_RATE_QUEUE_DEV": heart_rate_queue.queue_url,
                "HEALTH_SLEEP_QUEUE_DEV": sleep_queue.queue_url,
                "HEALTH_STEP_QUEUE_DEV": step_queue.queue_url,
                "HEALTH_OTHERS_QUEUE_DEV": others_queue.queue_url,
            }
        )

        self.heart_rate_function = lambda_.Function(
            self,
            "AlbanyHealthHeartRateLambdaFunctionDev",
            function_name="albanyHealth-heart-rate-lambda-function-dev",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-heart-rate-lambda-function"),
            timeout=Duration.seconds(300),  # 5 minutes
            memory_size=1024,  # 1024 MB - more memory for faster pandas processing
            layers=[lambda_.LayerVersion.from_layer_version_arn(
                self,
                "HeartRatePandasLayer",
                pandas_layer_arn
            )],
            environment={
                "DESTINATION_BUCKET": processed_bucket.bucket_name,
            }
        )

        self.step_function = lambda_.Function(
            self,
            "AlbanyHealthStepLambdaFunctionDev",
            function_name="albanyHealth-step-lambda-function-dev",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-step-lambda-function"),
            timeout=Duration.seconds(300),  # 5 minutes
            memory_size=1024,  # 1024 MB - more memory for faster pandas processing
            layers=[lambda_.LayerVersion.from_layer_version_arn(
                self,
                "StepsPandasLayer",
                pandas_layer_arn
            )],
            environment={
                "DESTINATION_BUCKET": processed_bucket.bucket_name,
            }
        )

        self.sleep_function = lambda_.Function(
            self,
            "AlbanyHealthSleepLambdaFunctionDev",
            function_name="albanyHealth-sleep-lambda-function-dev",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-sleep-lambda-function"),
            timeout=Duration.seconds(300),  # 5 minutes
            memory_size=1024,  # 1024 MB - more memory for faster pandas processing
            layers=[lambda_.LayerVersion.from_layer_version_arn(
                self,
                "SleepPandasLayer",
                pandas_layer_arn
            )],
            environment={
                "DESTINATION_BUCKET": processed_bucket.bucket_name,
            }
        )

        self.other_metrics_function = lambda_.Function(
            self,
            "AlbanyHealthOtherMetricsLambdaFunctionDev",
            function_name="albanyHealth-other-metrics-lambda-function-dev",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-other-metrics-lambda-function"),
            timeout=Duration.seconds(300),  # 5 minutes
            memory_size=1024,  # 1024 MB - more memory for faster pandas processing
            layers=[lambda_.LayerVersion.from_layer_version_arn(
                self,
                "OtherMetricsPandasLayer",
                pandas_layer_arn
            )],
            environment={
                "DESTINATION_BUCKET": processed_bucket.bucket_name,
            }
        )

        self.data_inactivity_checker_function = lambda_.Function(
            self,
            "AlbanyHealthDataInactivityCheckerLambdaFunctionDev",
            function_name="albanyHealth-data-inactivity-checker-lambda-function-dev",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.lambda_handler",
            code=lambda_.Code.from_asset(f"{services_base_path}/albanyHealth-data-inactivity-checker-lambda-function"),
            timeout=Duration.seconds(300),  # 5 minutes
            memory_size=512,  # 512 MB - moderate memory for batch processing
        )

        # Create an IAM policy statement for the SQSs queue to invoke the function
        invoke_policy_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["lambda:InvokeFunction"],
            resources=[self.main_router_function.function_arn,
                       self.heart_rate_function.function_arn,
                       self.other_metrics_function.function_arn,
                       self.sleep_function.function_arn,
                       self.step_function.function_arn]
        )

        # Grant the main router function permission to send messages to the heart rate, others, sleep, and step queues
        heart_rate_queue.grant_send_messages(self.main_router_function)
        others_queue.grant_send_messages(self.main_router_function)
        sleep_queue.grant_send_messages(self.main_router_function)
        step_queue.grant_send_messages(self.main_router_function)

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


        # Grant read access to the source_bucket for the heart rate, step, sleep, and other functions
        source_bucket.grant_read(self.heart_rate_function)
        source_bucket.grant_read(self.step_function)
        source_bucket.grant_read(self.sleep_function)
        source_bucket.grant_read(self.other_metrics_function)

        # Grant write access to the processed_bucket for the heart rate, step, sleep, and other functions
        processed_bucket.grant_write(self.heart_rate_function)
        processed_bucket.grant_write(self.step_function)
        processed_bucket.grant_write(self.sleep_function)
        processed_bucket.grant_write(self.other_metrics_function)
        
        # Grant read and write access to the processed_bucket for the data inactivity checker function
        # It needs ListBucket permission to check for batch.json files and read/write them
        processed_bucket.grant_read(self.data_inactivity_checker_function)
        processed_bucket.grant_write(self.data_inactivity_checker_function)



        # Apply removal policy to log groups
        # With useCdkManagedLogGroup flag enabled, CDK automatically creates log groups as CloudFormation resources
        # We explicitly set DESTROY removal policy so log groups are deleted when stack is deleted
        # This ensures clean stack deletion - log groups will be automatically removed with cdk destroy
        
        # Apply removal policy to all Lambda function log groups
        lambda_functions = [
            self.main_router_function,
            self.heart_rate_function,
            self.step_function,
            self.sleep_function,
            self.other_metrics_function,
            self.data_inactivity_checker_function
        ]
        
        for func in lambda_functions:
            # With useCdkManagedLogGroup, log groups are automatically created and managed by CDK
            # Access the log group and apply DESTROY removal policy
            # This ensures log groups are deleted when the stack is destroyed
            if hasattr(func, 'log_group') and func.log_group is not None:
                # Apply removal policy to the log group
                # This will ensure the log group is deleted on cdk destroy
                func.log_group.apply_removal_policy(RemovalPolicy.DESTROY)