from aws_cdk import (
    aws_sqs as sqs,
    Duration,
)
from constructs import Construct

class SQSQueues(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Lambda functions have a timeout of 300 seconds (5 minutes)
        # AWS requires SQS visibility timeout to be at least equal to Lambda timeout
        # Setting to Lambda timeout + 60 seconds buffer for immediate retries
        # This allows messages to retry as soon as Lambda fails (not waiting for timeout)
        visibility_timeout = Duration.seconds(360)  # 6 minutes = 300 + 60 seconds buffer
        
        # Dead-letter queue configuration
        # Messages that fail maxReceiveCount times will be moved to DLQ
        # Since failures should be rare, set to 5 retries to handle transient issues
        max_receive_count = 5  # Retry up to 5 times before moving to DLQ

        # Create dead-letter queues for all processing queues
        # These catch messages that fail repeatedly to prevent infinite retries
        main_dlq = sqs.Queue(
            self,
            "AlbanyHealthMain-DLQ-DEV",
            queue_name="AlbanyHealthMain-DLQ-DEV",
            retention_period=Duration.days(14),  # Keep failed messages for 14 days for debugging
        )
        
        sleep_dlq = sqs.Queue(
            self,
            "AlbanyHealthSleep-DLQ-DEV",
            queue_name="AlbanyHealthSleep-DLQ-DEV",
            retention_period=Duration.days(14),
        )
        
        step_dlq = sqs.Queue(
            self,
            "AlbanyHealthStep-DLQ-DEV",
            queue_name="AlbanyHealthStep-DLQ-DEV",
            retention_period=Duration.days(14),
        )
        
        others_dlq = sqs.Queue(
            self,
            "AlbanyHealthOthers-DLQ-DEV",
            queue_name="AlbanyHealthOthers-DLQ-DEV",
            retention_period=Duration.days(14),
        )
        
        heart_rate_dlq = sqs.Queue(
            self,
            "AlbanyHealthHeartRate-DLQ-DEV",
            queue_name="AlbanyHealthHeartRate-DLQ-DEV",
            retention_period=Duration.days(14),
        )
        
        processing_files_dlq = sqs.Queue(
            self,
            "AlbanyHealthSuccessfullProcessingFiles-DLQ-DEV",
            queue_name="AlbanyHealthSuccessfullProcessingFiles-DLQ-DEV",
            retention_period=Duration.days(14),
        )

        # Configure dead-letter queue settings
        dead_letter_queue = sqs.DeadLetterQueue(
            max_receive_count=max_receive_count,
            queue=main_dlq,
        )

        self.main_queue = sqs.Queue(
            self,
            "AlbanyHealthMain-SQSQueue-DEV",
            queue_name="AlbanyHealthMain-SQSQueue-DEV",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=dead_letter_queue,
        )

        self.sleep_queue = sqs.Queue(
            self,
            "AlbanyHealthSleep-SQSQueue-DEV",
            queue_name="AlbanyHealthSleep-SQSQueue-DEV",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=sleep_dlq,
            ),
        )

        self.step_queue = sqs.Queue(
            self,
            "AlbanyHealthStep-SQSQueue-DEV",
            queue_name="AlbanyHealthStep-SQSQueue-DEV",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=step_dlq,
            ),
        )

        self.others_queue = sqs.Queue(
            self,
            "AlbanyHealthOthers-SQSQueue-DEV",
            queue_name="AlbanyHealthOthers-SQSQueue-DEV",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=others_dlq,
            ),
        )

        self.heart_rate_queue = sqs.Queue(
            self,
            "AlbanyHealthHeartRate-SQSQueue-DEV",
            queue_name="AlbanyHealthHeartRate-SQSQueue-DEV",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=heart_rate_dlq,
            ),
        )

        self.processing_files_queue = sqs.Queue(
            self,
            "AlbanyHealthSuccessfullProcessingFiles-SQSQueue-DEV",
            queue_name="AlbanyHealthSuccessfullProcessingFiles-SQSQueue-DEV",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=processing_files_dlq,
            ),
        )