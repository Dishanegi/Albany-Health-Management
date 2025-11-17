from aws_cdk import (
    aws_s3 as s3,
    RemovalPolicy
)
from constructs import Construct

class S3Buckets(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.source_bucket = s3.Bucket(
            self,
            "AlbanyHealthSourceBucket",
            bucket_name="albanyhealthsource-s3bucket-dev",
            versioned=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.processed_bucket = s3.Bucket(
            self,
            "AlbanyHealthProcessedBucket",
            bucket_name="albanyhealthprocessed-s3bucket-dev",
            versioned=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.bbi_processing_bucket = s3.Bucket(
            self,
            "AlbanyHealthBBIProcessingBucket",
            bucket_name="albanyhealthbbiprocessing-s3bucket-dev",
            versioned=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.merged_bucket = s3.Bucket(
            self,
            "AlbanyHealthMergedBucket",
            bucket_name="albanyhealthmerged-s3bucket-dev",
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.bbi_merged_bucket = s3.Bucket(
            self,
            "AlbanyHealthBBIMergedBucket",
            bucket_name="albanyhealthbbimerged-s3bucket-dev",
            removal_policy=RemovalPolicy.DESTROY,
        )