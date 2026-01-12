import sys
import os

# Add infra directory to path for CDK imports
infra_path = os.path.join(os.path.dirname(__file__), '..', '..', 'infra')
infra_path = os.path.abspath(infra_path)
if infra_path not in sys.path:
    sys.path.insert(0, infra_path)

import aws_cdk as core
import aws_cdk.assertions as assertions

from albany_health_management.albany_health_management_stack import AlbanyHealthManagementStack

# example tests. To run these tests, uncomment this file along with the example
# resource in albany_health_management/albany_health_management_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AlbanyHealthManagementStack(app, "albany-health-management")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
