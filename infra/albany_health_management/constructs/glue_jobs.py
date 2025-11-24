from aws_cdk import (
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
            # Grant read access to source bucket
            s3_buckets.source_bucket.grant_read(glue_job_role)
            
            # Grant read/write access (includes GetObject, PutObject, ListBucket)
            s3_buckets.processed_bucket.grant_read_write(glue_job_role)
            s3_buckets.bbi_processing_bucket.grant_read_write(glue_job_role)
            s3_buckets.merged_bucket.grant_read_write(glue_job_role)
            s3_buckets.bbi_merged_bucket.grant_read_write(glue_job_role)
            
            # Explicitly grant delete permissions for buckets where delete operations are needed
            # grant_read_write should include DeleteObject, but we add it explicitly to be certain
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

        # Create Glue jobs using the L2 construct
        self.glue_jobs = {
            name: glue.CfnJob(
                self,
                f"{name}Job",
                name=name,
                command=glue.CfnJob.JobCommandProperty(
                    name="glueetl",
                    script_location=asset.s3_object_url,
                    python_version="3",
                ),
                role=glue_job_role.role_arn,
                glue_version="2.0",
            )
            for name, asset in script_assets.items()
        }