from aws_cdk import (
    aws_glue as glue,
    aws_cloudformation as cfn,
    custom_resources as cr,
)
from constructs import Construct
from ..config import EnvironmentConfig

class GlueWorkflows(Construct):
    def __init__(self, scope: Construct, construct_id: str, glue_jobs, activate_garmin_triggers_lambda, activate_bbi_triggers_lambda, environment: EnvironmentConfig = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Default to dev environment if not provided
        if environment is None:
            from ..config import get_environment
            environment = get_environment("dev")
        
        env_suffix = environment.name.lower()

        # Create Glue workflows with environment-specific naming
        garmin_workflow = glue.CfnWorkflow(
            self,
            "AlbanyHealthGarminHealthMetricsWorkflow",
            name=f"AlbanyHealthGarminHealthMetricsWorkflow-{env_suffix}",
        )

        bbi_workflow = glue.CfnWorkflow(
            self,
            "AlbanyHealthGarminBBIWorkflow",
            name=f"AlbanyHealthGarminBBIWorkflow-{env_suffix}",
        )

        # Expose workflows to other constructs (e.g., EventBridge rules)
        self.garmin_workflow = garmin_workflow
        self.bbi_workflow = bbi_workflow

        # Define the job order for the Garmin Health Metrics workflow
        # Job names must match the actual Glue job names (with environment suffix)
        garmin_job_order = [
            f"AlbanyHealthGarmin-Data-Preprocessor-Glue-Job-{env_suffix}",
            f"AlbanyHealthGarmin-Data-Merge-Glue-Job-{env_suffix}",
            f"AlbanyHealthGarmin-Data-Delete-Glue-Job-{env_suffix}",
        ]

        # Ensure all jobs exist before creating triggers
        # Add dependencies to ensure jobs are created before workflow references them
        all_garmin_jobs = [glue_jobs[job_name] for job_name in garmin_job_order]
        
        # Event-based trigger to allow EventBridge to start the Garmin workflow
        # EVENT triggers fire when EventBridge calls glue:NotifyEvent
        # This is the starting trigger for the workflow (only one starting trigger allowed)
        garmin_event_trigger = glue.CfnTrigger(
            self,
            f"GarminEventTrigger-{env_suffix.capitalize()}",
            type="EVENT",
            workflow_name=garmin_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=glue_jobs[garmin_job_order[0]].name
                )
            ],
        )
        
        # Ensure event trigger waits for all jobs to be created
        for job in all_garmin_jobs:
            garmin_event_trigger.add_dependency(job)

        # Create triggers to align the jobs in the Garmin Health Metrics workflow
        # CONDITIONAL triggers wait for the previous job to succeed before starting the next job
        # These triggers must be part of the workflow to ensure sequential execution
        # 
        # IMPORTANT: For CONDITIONAL triggers to work:
        # 1. workflow_name must match the workflow name exactly
        # 2. job_name in predicate must match the previous job name exactly (case-sensitive)
        # 3. state must be exactly "SUCCEEDED" (not "SUCCESS" or other values)
        # 4. All triggers must be in ACTIVATED state (check in AWS Console after deployment)
        garmin_conditional_triggers = []
        for i in range(len(garmin_job_order) - 1):
            current_job = glue_jobs[garmin_job_order[i]]
            next_job = glue_jobs[garmin_job_order[i + 1]]

            # CRITICAL: job_name must match current_job.name exactly
            # This is what the trigger waits for before starting next_job
            conditional_trigger = glue.CfnTrigger(
                self,
                f"GarminTrigger{i}-{env_suffix.capitalize()}",
                type="CONDITIONAL",
                workflow_name=garmin_workflow.name,  # Must match workflow name exactly
                actions=[glue.CfnTrigger.ActionProperty(job_name=next_job.name)],
                predicate=glue.CfnTrigger.PredicateProperty(
                    conditions=[
                        glue.CfnTrigger.ConditionProperty(
                            job_name=current_job.name,  # Must match previous job name exactly
                            state="SUCCEEDED",  # Must be exactly "SUCCEEDED"
                            logical_operator="EQUALS",
                        )
                    ],
                    logical="ANY",  # Changed from "AND" - though with single condition, both work the same
                ),
            )
            
            # Ensure trigger waits for both jobs to be created
            conditional_trigger.add_dependency(current_job)
            conditional_trigger.add_dependency(next_job)
            
            garmin_conditional_triggers.append(conditional_trigger)
            
            # Ensure conditional triggers are properly chained within the workflow
            # Each trigger depends on the previous one to maintain workflow structure
            if i == 0:
                # First conditional trigger depends on the event trigger
                conditional_trigger.add_dependency(garmin_event_trigger)
            else:
                # Subsequent conditional triggers depend on the previous conditional trigger
                conditional_trigger.add_dependency(garmin_conditional_triggers[i - 1])
        
        # Use the Lambda function passed from LambdaFunctions construct
        # This Lambda automatically activates CONDITIONAL triggers after deployment
        # Create custom resource provider to activate triggers after deployment
        activate_garmin_provider = cr.Provider(
            self,
            f"ActivateGarminTriggersProvider-{env_suffix.capitalize()}",
            on_event_handler=activate_garmin_triggers_lambda,
        )
        
        # Create the custom resource that will activate triggers
        # Using CfnCustomResource (L1 construct) with the provider's service token
        activate_garmin_resource = cfn.CfnCustomResource(
            self,
            f"ActivateGarminTriggersResource-{env_suffix.capitalize()}",
            service_token=activate_garmin_provider.service_token,
        )
        
        # Ensure this runs after all conditional triggers are created
        for trigger in garmin_conditional_triggers:
            activate_garmin_resource.add_dependency(trigger)

        # Define the job order for the BBI workflow
        # Job names must match the actual Glue job names (with environment suffix)
        bbi_job_order = [
            f"AlbanyHealthGarmin-BBI-Preprocessor-Glue-Job-{env_suffix}",
            f"AlbanyHealthGarmin-BBI-Merge-Data-Glue-Job-{env_suffix}",
            f"AlbanyHealthGarmin-BBI-Delete-Data-Glue-Job-{env_suffix}",
            f"AlbanyHealthGarmin-BBI-Delete-Source-Folders-Glue-Job-{env_suffix}",
        ]

        # Ensure all jobs exist before creating triggers
        # Add dependencies to ensure jobs are created before workflow references them
        all_bbi_jobs = [glue_jobs[job_name] for job_name in bbi_job_order]
        
        # Event-based trigger to allow EventBridge to start the BBI workflow
        # EVENT triggers fire when EventBridge calls glue:NotifyEvent
        # This is the starting trigger for the workflow (only one starting trigger allowed)
        bbi_event_trigger = glue.CfnTrigger(
            self,
            f"BBIEventTrigger-{env_suffix.capitalize()}",
            type="EVENT",
            workflow_name=bbi_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=glue_jobs[bbi_job_order[0]].name
                )
            ],
        )
        
        # Ensure event trigger waits for all jobs to be created
        for job in all_bbi_jobs:
            bbi_event_trigger.add_dependency(job)

        # Create triggers to align the jobs in the BBI workflow
        # CONDITIONAL triggers wait for the previous job to succeed before starting the next job
        # These triggers must be part of the workflow to ensure sequential execution
        #
        # IMPORTANT: For CONDITIONAL triggers to work:
        # 1. workflow_name must match the workflow name exactly
        # 2. job_name in predicate must match the previous job name exactly (case-sensitive)
        # 3. state must be exactly "SUCCEEDED" (not "SUCCESS" or other values)
        # 4. All triggers must be in ACTIVATED state (check in AWS Console after deployment)
        bbi_conditional_triggers = []
        for i in range(len(bbi_job_order) - 1):
            current_job = glue_jobs[bbi_job_order[i]]
            next_job = glue_jobs[bbi_job_order[i + 1]]

            # CRITICAL: job_name must match current_job.name exactly
            # This is what the trigger waits for before starting next_job
            conditional_trigger = glue.CfnTrigger(
                self,
                f"BBITrigger{i}-{env_suffix.capitalize()}",
                type="CONDITIONAL",
                workflow_name=bbi_workflow.name,  # Must match workflow name exactly
                actions=[glue.CfnTrigger.ActionProperty(job_name=next_job.name)],
                predicate=glue.CfnTrigger.PredicateProperty(
                    conditions=[
                        glue.CfnTrigger.ConditionProperty(
                            job_name=current_job.name,  # Must match previous job name exactly
                            state="SUCCEEDED",  # Must be exactly "SUCCEEDED"
                            logical_operator="EQUALS",
                        )
                    ],
                    logical="ANY",  # Changed from "AND" - though with single condition, both work the same
                ),
            )
            
            # Ensure trigger waits for both jobs to be created
            conditional_trigger.add_dependency(current_job)
            conditional_trigger.add_dependency(next_job)
            
            bbi_conditional_triggers.append(conditional_trigger)
            
            # Ensure conditional triggers are properly chained within the workflow
            # Each trigger depends on the previous one to maintain workflow structure
            if i == 0:
                # First conditional trigger depends on the event trigger
                conditional_trigger.add_dependency(bbi_event_trigger)
            else:
                # Subsequent conditional triggers depend on the previous conditional trigger
                conditional_trigger.add_dependency(bbi_conditional_triggers[i - 1])
        
        # Use the Lambda function passed from LambdaFunctions construct
        # This Lambda automatically activates BBI CONDITIONAL triggers after deployment
        # Create custom resource to activate triggers after all conditional triggers are created
        activate_bbi_provider = cr.Provider(
            self,
            f"ActivateBBITriggersProvider-{env_suffix.capitalize()}",
            on_event_handler=activate_bbi_triggers_lambda,
        )
        
        # Create the custom resource that will activate triggers
        # Using CfnCustomResource (L1 construct) with the provider's service token
        activate_bbi_resource = cfn.CfnCustomResource(
            self,
            f"ActivateBBITriggersResource-{env_suffix.capitalize()}",
            service_token=activate_bbi_provider.service_token,
        )
        
        # Ensure this runs after all conditional triggers are created
        for trigger in bbi_conditional_triggers:
            activate_bbi_resource.add_dependency(trigger)