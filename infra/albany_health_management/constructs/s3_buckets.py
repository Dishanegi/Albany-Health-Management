from aws_cdk import (
    aws_s3 as s3,
    RemovalPolicy
)
from constructs import Construct
from ..config import EnvironmentConfig

class S3Buckets(Construct):
    def __init__(self, scope: Construct, construct_id: str, environment: EnvironmentConfig = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Default to dev environment if not provided
        if environment is None:
            from ..config import get_environment
            environment = get_environment("dev")
        
        env_suffix = environment.name.lower()

        self.source_bucket = s3.Bucket(
            self,
            "AlbanyHealthSourceBucket",
            bucket_name=f"albanyhealthsource-s3bucket-{env_suffix}",
            versioned=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.processed_bucket = s3.Bucket(
            self,
            "AlbanyHealthProcessedBucket",
            bucket_name=f"albanyhealthprocessed-s3bucket-{env_suffix}",
            versioned=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.bbi_processing_bucket = s3.Bucket(
            self,
            "AlbanyHealthBBIProcessingBucket",
            bucket_name=f"albanyhealthbbiprocessing-s3bucket-{env_suffix}",
            versioned=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.merged_bucket = s3.Bucket(
            self,
            "AlbanyHealthMergedBucket",
            bucket_name=f"albanyhealthmerged-s3bucket-{env_suffix}",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.bbi_merged_bucket = s3.Bucket(
            self,
            "AlbanyHealthBBIMergedBucket",
            bucket_name=f"albanyhealthbbimerged-s3bucket-{env_suffix}",
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )