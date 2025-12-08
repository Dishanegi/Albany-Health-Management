from aws_cdk import (
    Stack,
    aws_events as events,
    aws_iam as iam,
)
from constructs import Construct
from ..config import EnvironmentConfig

class EventBridgeRules(Construct):
    def __init__(self, scope: Construct, construct_id: str, data_inactivity_checker_function, glue_workflows, environment: EnvironmentConfig = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Default to dev environment if not provided
        if environment is None:
            from ..config import get_environment
            environment = get_environment("dev")
        
        env_suffix = environment.name.lower()

        # Allow the data inactivity checker Lambda to publish events that drive the workflows
        data_inactivity_checker_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["events:PutEvents"],
                resources=["*"],
            )
        )

        stack = Stack.of(self)
        bbi_workflow_arn = stack.format_arn(
            service="glue",
            resource="workflow",
            resource_name=glue_workflows.bbi_workflow.name,
        )
        garmin_workflow_arn = stack.format_arn(
            service="glue",
            resource="workflow",
            resource_name=glue_workflows.garmin_workflow.name,
        )

        # Create a role for EventBridge to start Glue workflows with environment-specific naming
        eventbridge_to_glue_role = iam.Role(
            self,
            f"EventBridgeToGlueRole-{env_suffix.capitalize()}",
            role_name=f"AlbanyHealthEventBridgeToGlueRole-{env_suffix}",
            assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
        )

        # EventBridge needs glue:NotifyEvent permission to trigger Glue workflows
        eventbridge_to_glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:NotifyEvent"],
                resources=[
                    bbi_workflow_arn,
                    garmin_workflow_arn,
                ],
            )
        )

        # Create EventBridge rules to trigger Glue workflows
        # These exact names will be used in AWS EventBridge console
        completion_rule_name = f"trigger-merge-patient-health-metrics-{env_suffix}"
        processing_rule_name = f"trigger-patients-bbi-flow-{env_suffix}"
        
        # Environment-specific detail-types to ensure events only match this environment's rules
        bbi_detail_type = f"AlbanyHealthGarminBBIWorkflow-{env_suffix}"
        garmin_detail_type = f"AlbanyHealthGarminHealthMetricsWorkflow-{env_suffix}"
        
        # Create EventBridge rule for BBI workflow processing
        # The 'name' parameter ensures this exact name is used in AWS
        self.garmin_bbi_workflow_rule = events.CfnRule(
            self,
            f"GarminBBIWorkflowRule-{env_suffix.capitalize()}",
            name=processing_rule_name,  # This sets the exact rule name in AWS
            description=f"Trigger AlbanyHealthGarminBBIWorkflow-{env_suffix}",
            state="ENABLED",  # Explicitly enable the rule
            event_pattern={
                "source": ["lambda"],
                "detail-type": [bbi_detail_type],
            },
            targets=[
                events.CfnRule.TargetProperty(
                    arn=bbi_workflow_arn,
                    id=f"GarminBBIWorkflowTarget-{env_suffix}",
                    role_arn=eventbridge_to_glue_role.role_arn,
                )
            ],
        )

        # Create EventBridge rule for Garmin Health Metrics workflow completion
        # The 'name' parameter ensures this exact name is used in AWS
        self.garmin_health_metrics_workflow_rule = events.CfnRule(
            self,
            f"GarminHealthMetricsWorkflowRule-{env_suffix.capitalize()}",
            name=completion_rule_name,  # This sets the exact rule name in AWS
            description=f"Trigger AlbanyHealthGarminHealthMetricsWorkflow-{env_suffix}",
            state="ENABLED",  # Explicitly enable the rule
            event_pattern={
                "source": ["lambda"],
                "detail-type": [garmin_detail_type],
            },
            targets=[
                events.CfnRule.TargetProperty(
                    arn=garmin_workflow_arn,
                    id=f"GarminHealthMetricsWorkflowTarget-{env_suffix}",
                    role_arn=eventbridge_to_glue_role.role_arn,
                )
            ],
        )
        
        # Store rule names and detail-types for Lambda environment variables
        # These are the exact names that will appear in AWS EventBridge console
        self.completion_rule_name = completion_rule_name
        self.processing_rule_name = processing_rule_name
        self.bbi_detail_type = bbi_detail_type
        self.garmin_detail_type = garmin_detail_type