from aws_cdk import (
    Stack,
    aws_events as events,
    aws_iam as iam,
)
from constructs import Construct

class EventBridgeRules(Construct):
    def __init__(self, scope: Construct, construct_id: str, data_inactivity_checker_function, glue_workflows, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

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

        # Create a role for EventBridge to start Glue workflows
        eventbridge_to_glue_role = iam.Role(
            self,
            "EventBridgeToGlueRole",
            assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
        )

        eventbridge_to_glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:StartWorkflowRun"],
                resources=[
                    bbi_workflow_arn,
                    garmin_workflow_arn,
                ],
            )
        )

        # Create EventBridge rules to trigger Glue workflows
        self.garmin_bbi_workflow_rule = events.CfnRule(
            self,
            "GarminBBIWorkflowRule",
            description="Trigger AlbanyHealthGarminBBIWorkflow",
            event_pattern={
                "source": ["lambda"],
                "detail-type": ["AlbanyHealthGarminBBIWorkflow"],
            },
            targets=[
                events.CfnRule.TargetProperty(
                    arn=bbi_workflow_arn,
                    id="GarminBBIWorkflowTarget",
                    role_arn=eventbridge_to_glue_role.role_arn,
                )
            ],
        )

        self.garmin_health_metrics_workflow_rule = events.CfnRule(
            self,
            "GarminHealthMetricsWorkflowRule",
            description="Trigger AlbanyHealthGarminHealthMetricsWorkflow",
            event_pattern={
                "source": ["lambda"],
                "detail-type": ["AlbanyHealthGarminHealthMetricsWorkflow"],
            },
            targets=[
                events.CfnRule.TargetProperty(
                    arn=garmin_workflow_arn,
                    id="GarminHealthMetricsWorkflowTarget",
                    role_arn=eventbridge_to_glue_role.role_arn,
                )
            ],
        )