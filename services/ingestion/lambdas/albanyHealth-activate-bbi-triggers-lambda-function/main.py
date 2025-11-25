import boto3
import json
import cfnresponse

def handler(event, context):
    """
    Custom resource Lambda to automatically activate Glue CONDITIONAL triggers
    for the BBI workflow after CDK deployment.
    """
    glue_client = boto3.client('glue')
    
    try:
        request_type = event['RequestType']
        
        if request_type in ['Create', 'Update']:
            # Get all triggers for the BBI workflow
            response = glue_client.get_triggers()
            activated_count = 0
            
            for trigger in response.get('Triggers', []):
                trigger_name = trigger['Name']
                # Check if this is one of our CONDITIONAL triggers for BBI workflow
                if (trigger.get('Type') == 'CONDITIONAL' and 
                    trigger.get('WorkflowName') == 'AlbanyHealthGarminBBIWorkflow' and
                    ('BBITrigger0' in trigger_name or 'BBITrigger1' in trigger_name)):
                    
                    # Check current state
                    if trigger.get('State') in ['CREATED', 'DEACTIVATED']:
                        try:
                            glue_client.start_trigger(Name=trigger_name)
                            activated_count += 1
                            print(f"Activated trigger: {trigger_name}")
                        except Exception as e:
                            print(f"Error activating {trigger_name}: {str(e)}")
            
            print(f"Activated {activated_count} BBI triggers")
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {
                'Message': f'Activated {activated_count} triggers'
            })
        else:
            # Delete - nothing to do
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
            
    except Exception as e:
        print(f"Error: {str(e)}")
        cfnresponse.send(event, context, cfnresponse.FAILED, {
            'Error': str(e)
        })

