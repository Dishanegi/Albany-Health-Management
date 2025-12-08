import boto3
import json
import cfnresponse
import os

def handler(event, context):
    """
    Custom resource Lambda to automatically activate Glue CONDITIONAL triggers
    for the BBI workflow after CDK deployment.
    Uses environment-specific workflow name from environment variable.
    """
    glue_client = boto3.client('glue')
    
    try:
        # Get environment-specific workflow name from environment variable
        workflow_name = os.environ.get('WORKFLOW_NAME', 'AlbanyHealthGarminBBIWorkflow')
        env_suffix = os.environ.get('ENVIRONMENT', '')
        
        request_type = event['RequestType']
        
        if request_type in ['Create', 'Update']:
            # Get all triggers for the BBI workflow
            response = glue_client.get_triggers()
            activated_count = 0
            
            print(f"Looking for triggers for workflow: {workflow_name}")
            print(f"Environment suffix: {env_suffix}")
            
            for trigger in response.get('Triggers', []):
                trigger_name = trigger['Name']
                trigger_workflow = trigger.get('WorkflowName', '')
                
                # Check if this is one of our CONDITIONAL triggers for BBI workflow
                # Match by workflow name (environment-specific) and trigger name pattern
                is_conditional = trigger.get('Type') == 'CONDITIONAL'
                matches_workflow = trigger_workflow == workflow_name
                matches_trigger_pattern = 'BBITrigger' in trigger_name
                
                if is_conditional and matches_workflow and matches_trigger_pattern:
                    # Check current state
                    if trigger.get('State') in ['CREATED', 'DEACTIVATED']:
                        try:
                            glue_client.start_trigger(Name=trigger_name)
                            activated_count += 1
                            print(f"Activated trigger: {trigger_name} for workflow: {workflow_name}")
                        except Exception as e:
                            print(f"Error activating {trigger_name}: {str(e)}")
                    else:
                        print(f"Trigger {trigger_name} already in state: {trigger.get('State')}")
            
            print(f"Activated {activated_count} BBI triggers for workflow: {workflow_name}")
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                'Message': f'Activated {activated_count} triggers for {workflow_name}'
            })
        else:
            # Delete - nothing to do
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
            
    except Exception as e:
        print(f"Error: {str(e)}")
        cfnresponse.send(event, context, cfnresponse.FAILED, {
            'Error': str(e)
        })

