from aws_cdk import (
    aws_dynamodb as dynamodb,
    RemovalPolicy,
)
from constructs import Construct
from ..config import EnvironmentConfig


class DynamoDBTables(Construct):
    def __init__(self, scope: Construct, construct_id: str, environment: EnvironmentConfig = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        if environment is None:
            from ..config import get_environment
            environment = get_environment("dev")

        env_suffix = environment.name.lower()

        # Batch tracking table — replaces S3 batch.json for race-condition-free file counting
        #
        # Simple primary key:
        #   PK  patient_id  (e.g. "Testing-1_cd037752#2025-02-04")
        #
        # The batch_date (derived from the YYMMDD filename prefix) is appended to the
        # patient_id string so that each patient's weekly submission gets its own row
        # without needing a separate sort key.
        # Uses atomic ADD operations so concurrent Lambda invocations never lose a file count.
        # Survey batch tracking table — same atomic counting pattern as health metrics
        # PK: patient_id = "Testing-1_cd037752#2025-04-27"
        # Tracks questionnaire files per patient per day
        self.survey_batch_tracking_table = dynamodb.Table(
            self,
            "AlbanyHealthSurveyBatchTrackingTable",
            table_name=f"AlbanyHealthSurveyBatchTracking-{env_suffix}",
            partition_key=dynamodb.Attribute(
                name="patient_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.batch_tracking_table = dynamodb.Table(
            self,
            "AlbanyHealthBatchTrackingTable",
            table_name=f"AlbanyHealthBatchTracking-{env_suffix}",
            partition_key=dynamodb.Attribute(
                name="patient_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
