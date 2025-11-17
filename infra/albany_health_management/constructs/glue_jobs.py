from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    aws_s3_assets as s3_assets,
)
from constructs import Construct

class GlueJobs(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an IAM role for the Glue jobs
        glue_job_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            # Attach necessary permissions to the role
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