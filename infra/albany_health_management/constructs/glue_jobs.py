import os
from pathlib import Path
from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3_assets as s3_assets,
)
from constructs import Construct
from ..config import EnvironmentConfig

class GlueJobs(Construct):
    def __init__(self, scope: Construct, construct_id: str, s3_buckets=None, environment: EnvironmentConfig = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Default to dev environment if not provided
        if environment is None:
            from ..config import get_environment
            environment = get_environment("dev")
        
        self.environment = environment
        env_suffix = environment.name.lower()

        # Create an IAM role for the Glue jobs with environment-specific naming
        glue_job_role = iam.Role(
            self,
            f"GlueJobRole-{env_suffix.capitalize()}",
            role_name=f"AlbanyHealthGlueJobRole-{env_suffix}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        
        # Grant admin access to all Glue jobs
        # This provides full AWS access for the Glue job role
        glue_job_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
        )
        
        # Grant S3 permissions to Glue jobs if buckets are provided
        if s3_buckets:
            # Grant read access to source bucket (includes GetObject and ListBucket)
            s3_buckets.source_bucket.grant_read(glue_job_role)
            
            # Grant read/write access (includes GetObject, PutObject, ListBucket)
            s3_buckets.processed_bucket.grant_read_write(glue_job_role)
            s3_buckets.bbi_processing_bucket.grant_read_write(glue_job_role)
            s3_buckets.merged_bucket.grant_read_write(glue_job_role)
            s3_buckets.bbi_merged_bucket.grant_read_write(glue_job_role)
            
            # Explicitly grant ListBucket permissions for all buckets
            glue_job_role.add_to_policy(
                iam.PolicyStatement(
                    actions=["s3:ListBucket"],
                    resources=[
                        s3_buckets.source_bucket.bucket_arn,
                        s3_buckets.processed_bucket.bucket_arn,
                        s3_buckets.bbi_processing_bucket.bucket_arn,
                        s3_buckets.merged_bucket.bucket_arn,
                        s3_buckets.bbi_merged_bucket.bucket_arn,
                    ],
                )
            )
            
            # Explicitly grant delete permissions for buckets where delete operations are needed
            glue_job_role.add_to_policy(
                iam.PolicyStatement(
                    actions=["s3:DeleteObject"],
                    resources=[
                        s3_buckets.source_bucket.arn_for_objects("*"),  # For deleting source folders
                        s3_buckets.processed_bucket.arn_for_objects("*"),
                        s3_buckets.bbi_processing_bucket.arn_for_objects("*"),
                        s3_buckets.merged_bucket.arn_for_objects("*"),
                        s3_buckets.bbi_merged_bucket.arn_for_objects("*"),
                    ],
                )
            )
        else:
            # Fallback: Grant broad S3 permissions if buckets not provided
            # This is less secure but ensures jobs can access S3
            glue_job_role.add_to_policy(
                iam.PolicyStatement(
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                    ],
                    resources=["*"],
                )
            )
        
        # Grant CloudWatch Logs permissions for Glue job logging
        glue_job_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )
        
        # Grant permission to read Glue scripts from CDK assets bucket
        # CDK uploads the scripts to an assets bucket, and Glue needs to read them
        stack = Stack.of(self)
        glue_job_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[
                    f"arn:aws:s3:::cdk-*-assets-{stack.account}-{stack.region}/*",
                ],
            )
        )
        glue_job_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[
                    f"arn:aws:s3:::cdk-*-assets-{stack.account}-{stack.region}",
                ],
            )
        )

        # Define the script names and paths
        # Get absolute path to services directory (relative to project root)
        # This file is in: infra/albany_health_management/constructs/glue_jobs.py
        # Project root is: infra/../ (two levels up)
        current_file = Path(__file__).resolve()
        project_root = current_file.parent.parent.parent.parent
        glue_scripts_base_path = str(project_root / "services" / "analytics" / "glue_jobs")

        # Job names with environment suffix and their corresponding script paths
        script_names_and_paths = [
            (f"AlbanyHealthGarmin-Data-Preprocessor-Glue-Job-{env_suffix}", f"{glue_scripts_base_path}/AlbanyHealth-Garmin-Health-metrics-Flow-Script/Garmin-Data-preprocessing-script.py"),
            (f"AlbanyHealthGarmin-Data-Merge-Glue-Job-{env_suffix}", f"{glue_scripts_base_path}/AlbanyHealth-Garmin-Health-metrics-Flow-Script/Garmin-Data-merge-script.py"),
            (f"AlbanyHealthGarmin-Data-Delete-Glue-Job-{env_suffix}", f"{glue_scripts_base_path}/AlbanyHealth-Garmin-Health-metrics-Flow-Script/Garmin-Data-delete-script.py"),
            (f"AlbanyHealthGarmin-BBI-Preprocessor-Glue-Job-{env_suffix}", f"{glue_scripts_base_path}/AlbanyHealth-BBI-Flow-Script/BBI-preprocessing-data-script.py"),
            (f"AlbanyHealthGarmin-BBI-Merge-Data-Glue-Job-{env_suffix}", f"{glue_scripts_base_path}/AlbanyHealth-BBI-Flow-Script/BBI-merge-data-script.py"),
            (f"AlbanyHealthGarmin-BBI-Delete-Data-Glue-Job-{env_suffix}", f"{glue_scripts_base_path}/AlbanyHealth-BBI-Flow-Script/BBI-delete-data-script.py"),
            (f"AlbanyHealthGarmin-BBI-Delete-Source-Folders-Glue-Job-{env_suffix}", f"{glue_scripts_base_path}/AlbanyHealth-BBI-Flow-Script/BBI-delete-source-folders-script.py"),
        ]

        # Upload the ETL scripts to S3 using assets with environment-specific naming
        script_assets = {
            name: s3_assets.Asset(self, f"{name}Asset-{env_suffix.capitalize()}", path=path)
            for name, path in script_names_and_paths
        }

        # Define default arguments for each Glue job with bucket names from environment variables
        # Only set defaults if s3_buckets are provided
        # Use base job names (without env suffix) for lookup, then map to full job names
        job_default_arguments = {}
        if s3_buckets:
            # Map base job names to their default arguments
            base_job_args = {
                "AlbanyHealthGarmin-Data-Preprocessor-Glue-Job": {
                    "--SOURCE_BUCKET": s3_buckets.processed_bucket.bucket_name,
                    "--TARGET_BUCKET": s3_buckets.merged_bucket.bucket_name,
                },
                "AlbanyHealthGarmin-Data-Merge-Glue-Job": {
                    "--SOURCE_BUCKET": s3_buckets.processed_bucket.bucket_name,
                    "--TARGET_BUCKET": s3_buckets.merged_bucket.bucket_name,
                },
                "AlbanyHealthGarmin-Data-Delete-Glue-Job": {
                    "--BUCKET_NAME": s3_buckets.processed_bucket.bucket_name,
                },
                "AlbanyHealthGarmin-BBI-Preprocessor-Glue-Job": {
                    "--SOURCE_BUCKET": s3_buckets.source_bucket.bucket_name,
                    "--DESTINATION_BUCKET": s3_buckets.bbi_processing_bucket.bucket_name,
                },
                "AlbanyHealthGarmin-BBI-Merge-Data-Glue-Job": {
                    "--SOURCE_BUCKET": s3_buckets.bbi_processing_bucket.bucket_name,
                    "--DESTINATION_BUCKET": s3_buckets.bbi_merged_bucket.bucket_name,
                },
                "AlbanyHealthGarmin-BBI-Delete-Data-Glue-Job": {
                    "--BUCKET_NAME": s3_buckets.bbi_processing_bucket.bucket_name,
                },
                "AlbanyHealthGarmin-BBI-Delete-Source-Folders-Glue-Job": {
                    "--SOURCE_BUCKET": s3_buckets.source_bucket.bucket_name,
                },
            }
            
            # Map full job names (with env suffix) to their arguments
            for base_name, args in base_job_args.items():
                full_name = f"{base_name}-{env_suffix}"
                job_default_arguments[full_name] = args

        # Create Glue jobs using the L2 construct
        # All jobs run as glueetl (PySpark) with identical configuration and access
        self.glue_jobs = {}
        for name, asset in script_assets.items():
            # All jobs are configured as glueetl (PySpark) jobs with identical settings
            job_props = {
                "name": name,
                "command": glue.CfnJob.JobCommandProperty(
                    name="glueetl",  # All jobs run as PySpark (glueetl)
                    script_location=asset.s3_object_url,
                    python_version="3",
                ),
                "role": glue_job_role.role_arn,  # Same IAM role for all jobs
                "glue_version": "5.0",  # Same Glue version for all jobs
                # Configure worker type and number of workers (required for glueetl)
                # Using G.1X worker type (4 vCPU, 16 GB memory) with 10 workers
                "worker_type": "G.1X",
                "number_of_workers": 10,
            }
            
            # Add default arguments if buckets are provided and job has defaults defined
            if s3_buckets and name in job_default_arguments:
                job_props["default_arguments"] = job_default_arguments[name]
            
            # Create the Glue job with identical configuration for all jobs
            self.glue_jobs[name] = glue.CfnJob(
                self,
                f"{name}Job-{env_suffix.capitalize()}",
                **job_props
            )