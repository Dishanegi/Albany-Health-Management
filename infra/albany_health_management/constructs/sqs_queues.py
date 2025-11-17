from aws_cdk import (
    aws_sqs as sqs,
)
from constructs import Construct

class SQSQueues(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.main_queue = sqs.Queue(
            self,
            "AlbanyHealthMain-SQSQueue-DEV",
            queue_name="AlbanyHealthMain-SQSQueue-DEV",
        )

        self.sleep_queue = sqs.Queue(
            self,
            "AlbanyHealthSleep-SQSQueue-DEV",
            queue_name="AlbanyHealthSleep-SQSQueue-DEV",
        )

        self.step_queue = sqs.Queue(
            self,
            "AlbanyHealthStep-SQSQueue-DEV",
            queue_name="AlbanyHealthStep-SQSQueue-DEV",
        )

        self.others_queue = sqs.Queue(
            self,
            "AlbanyHealthOthers-SQSQueue-DEV",
            queue_name="AlbanyHealthOthers-SQSQueue-DEV",
        )

        self.heart_rate_queue = sqs.Queue(
            self,
            "AlbanyHealthHeartRate-SQSQueue-DEV",
            queue_name="AlbanyHealthHeartRate-SQSQueue-DEV",
        )

        self.processing_files_queue = sqs.Queue(
            self,
            "AlbanyHealthSuccessfullProcessingFiles-SQSQueue-DEV",
            queue_name="AlbanyHealthSuccessfullProcessingFiles-SQSQueue-DEV",
        )