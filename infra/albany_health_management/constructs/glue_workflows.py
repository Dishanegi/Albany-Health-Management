from aws_cdk import (
    aws_glue as glue,
)
from constructs import Construct

class GlueWorkflows(Construct):
    def __init__(self, scope: Construct, construct_id: str, glue_jobs, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Glue workflows
        garmin_workflow = glue.CfnWorkflow(
            self,
            "AlbanyHealthGarminHealthMetricsWorkflow",
            name="AlbanyHealthGarminHealthMetricsWorkflow",
        )

        bbi_workflow = glue.CfnWorkflow(
            self,
            "AlbanyHealthGarminBBIWorkflow",
            name="AlbanyHealthGarminBBIWorkflow",
        )

        # Expose workflows to other constructs (e.g., EventBridge rules)
        self.garmin_workflow = garmin_workflow
        self.bbi_workflow = bbi_workflow

        # Define the job order for the Garmin Health Metrics workflow
        garmin_job_order = [
            "AlbanyHealthGarmin-Data-Preprocessor-Glue-Job",
            "AlbanyHealthGarmin-Data-Merge-Glue-Job",
            "AlbanyHealthGarmin-Data-Delete-Glue-Job",
        ]

        # Event-based trigger to allow EventBridge to start the Garmin workflow
        # EVENT triggers fire when EventBridge calls glue:NotifyEvent
        # This is the starting trigger for the workflow (only one starting trigger allowed)
        glue.CfnTrigger(
            self,
            "GarminEventTrigger",
            type="EVENT",
            workflow_name=garmin_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=glue_jobs[garmin_job_order[0]].name
                )
            ],
        )

        # Create triggers to align the jobs in the Garmin Health Metrics workflow
        for i in range(len(garmin_job_order) - 1):
            current_job = glue_jobs[garmin_job_order[i]]
            next_job = glue_jobs[garmin_job_order[i + 1]]

            glue.CfnTrigger(
                self,
                f"GarminTrigger{i}",
                type="CONDITIONAL",
                workflow_name=garmin_workflow.name,
                actions=[glue.CfnTrigger.ActionProperty(job_name=next_job.name)],
                predicate=glue.CfnTrigger.PredicateProperty(
                    conditions=[
                        glue.CfnTrigger.ConditionProperty(
                            job_name=current_job.name,
                            state="SUCCEEDED",
                            logical_operator="EQUALS",
                        )
                    ],
                    logical="AND",
                ),
            )

        # Define the job order for the BBI workflow
        bbi_job_order = [
            "AlbanyHealthGarmin-BBI-Preprocessor-Glue-Job",
            "AlbanyHealthGarmin-BBI-Merge-Data-Glue-Job",
            "AlbanyHealthGarmin-BBI-Delete-Data-Glue-Job",
        ]

        # Event-based trigger to allow EventBridge to start the BBI workflow
        # EVENT triggers fire when EventBridge calls glue:NotifyEvent
        # This is the starting trigger for the workflow (only one starting trigger allowed)
        glue.CfnTrigger(
            self,
            "BBIEventTrigger",
            type="EVENT",
            workflow_name=bbi_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=glue_jobs[bbi_job_order[0]].name
                )
            ],
        )

        # Create triggers to align the jobs in the BBI workflow
        for i in range(len(bbi_job_order) - 1):
            current_job = glue_jobs[bbi_job_order[i]]
            next_job = glue_jobs[bbi_job_order[i + 1]]

            glue.CfnTrigger(
                self,
                f"BBITrigger{i}",
                type="CONDITIONAL",
                workflow_name=bbi_workflow.name,
                actions=[glue.CfnTrigger.ActionProperty(job_name=next_job.name)],
                predicate=glue.CfnTrigger.PredicateProperty(
                    conditions=[
                        glue.CfnTrigger.ConditionProperty(
                            job_name=current_job.name,
                            state="SUCCEEDED",
                            logical_operator="EQUALS",
                        )
                    ],
                    logical="AND",
                ),
            )