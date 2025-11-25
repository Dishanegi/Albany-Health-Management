from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3_assets as s3_assets,
)
from constructs import Construct

class GlueJobs(Construct):
    def __init__(self, scope: Construct, construct_id: str, s3_buckets=None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an IAM role for the Glue jobs
        glue_job_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
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
        glue_scripts_base_path = "../services/analytics/glue_jobs"

        script_names_and_paths = [
            ("AlbanyHealthGarmin-Data-Preprocessor-Glue-Job", f"{glue_scripts_base_path}/AlbanyHealth-Garmin-Health-metrics-Flow-Script/Garmin-Data-preprocessing-script.py"),
            ("AlbanyHealthGarmin-Data-Merge-Glue-Job", f"{glue_scripts_base_path}/AlbanyHealth-Garmin-Health-metrics-Flow-Script/Garmin-Data-merge-script.py"),
            ("AlbanyHealthGarmin-Data-Delete-Glue-Job", f"{glue_scripts_base_path}/AlbanyHealth-Garmin-Health-metrics-Flow-Script/Garmin-Data-delete-script.py"),
            ("AlbanyHealthGarmin-BBI-Preprocessor-Glue-Job", f"{glue_scripts_base_path}/AlbanyHealth-BBI-Flow-Script/BBI-preprocessing-data-script.py"),
            ("AlbanyHealthGarmin-BBI-Merge-Data-Glue-Job", f"{glue_scripts_base_path}/AlbanyHealth-BBI-Flow-Script/BBI-merge-data-script.py"),
            ("AlbanyHealthGarmin-BBI-Delete-Data-Glue-Job", f"{glue_scripts_base_path}/AlbanyHealth-BBI-Flow-Script/BBI-delete-data-script.py"),
        ]

        # Upload the ETL scripts to S3 using assets
        script_assets = {
            name: s3_assets.Asset(self, f"{name}Asset", path=path)
            for name, path in script_names_and_paths
        }

        # Define default arguments for each Glue job with bucket names from environment variables
        # Only set defaults if s3_buckets are provided
        job_default_arguments = {}
        if s3_buckets:
            job_default_arguments = {
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
            }

        # Create Glue jobs using the L2 construct
        self.glue_jobs = {}
        for name, asset in script_assets.items():
            job_props = {
                "name": name,
                "command": glue.CfnJob.JobCommandProperty(
                    name="glueetl",
                    script_location=asset.s3_object_url,
                    python_version="3",
                ),
                "role": glue_job_role.role_arn,
                "glue_version": "5.0",
            }
            
            # Add default arguments if buckets are provided and job has defaults defined
            if s3_buckets and name in job_default_arguments:
                job_props["default_arguments"] = job_default_arguments[name]
            
            self.glue_jobs[name] = glue.CfnJob(
                self,
                f"{name}Job",
                **job_props
            )