#!/usr/bin/env python3
import os
import warnings

# Suppress typeguard warnings from AWS CDK (these are harmless)
warnings.filterwarnings("ignore", category=UserWarning, module="aws_cdk")

import aws_cdk as cdk

from albany_health_management.albany_health_management_stack import AlbanyHealthManagementStack
from albany_health_management.config import get_environment

app = cdk.App()

# Get environment from context or default to 'dev'
# Usage: cdk deploy --context env=staging
# Or: cdk deploy --context env=prod
env_name = app.node.try_get_context("env") or os.getenv("CDK_ENV", "dev")
environment = get_environment(env_name)

# Get account and region from context, environment variables, or use defaults
account = (
    app.node.try_get_context("account") 
    or environment.account 
    or os.getenv("CDK_DEFAULT_ACCOUNT")
)
region = (
    app.node.try_get_context("region") 
    or environment.region 
    or os.getenv("CDK_DEFAULT_REGION", "us-east-1")
)

# Create CDK environment object
cdk_env = None
if account and region:
    cdk_env = cdk.Environment(account=account, region=region)

# Stack name includes environment for clarity
# This ensures each environment gets its own stack
stack_name = f"AlbanyHealthManagementStack-{environment.name}"

# Print environment info for debugging
print(f"Deploying to environment: {environment.name}")
print(f"Stack name: {stack_name}")
if cdk_env:
    print(f"Account: {cdk_env.account}, Region: {cdk_env.region}")
else:
    print("Using default account/region from AWS CLI configuration")

AlbanyHealthManagementStack(
    app, 
    stack_name,
    env=cdk_env,
    env_config=environment,
)

# Add tags to all resources in the stack
cdk.Tags.of(app).add("Environment", environment.name)
cdk.Tags.of(app).add("Project", "AlbanyHealthManagement")

app.synth()
