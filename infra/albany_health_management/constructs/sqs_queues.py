from aws_cdk import (
    aws_sqs as sqs,
    Duration,
)
from constructs import Construct
from ..config import EnvironmentConfig

class SQSQueues(Construct):
    def __init__(self, scope: Construct, construct_id: str, environment: EnvironmentConfig = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Default to dev environment if not provided
        if environment is None:
            from ..config import get_environment
            environment = get_environment("dev")
        
        env_suffix = environment.name.upper()

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
            f"AlbanyHealthMain-DLQ-{env_suffix}",
            queue_name=f"AlbanyHealthMain-DLQ-{env_suffix}",
            retention_period=Duration.days(14),  # Keep failed messages for 14 days for debugging
        )
        
        sleep_dlq = sqs.Queue(
            self,
            f"AlbanyHealthSleep-DLQ-{env_suffix}",
            queue_name=f"AlbanyHealthSleep-DLQ-{env_suffix}",
            retention_period=Duration.days(14),
        )
        
        step_dlq = sqs.Queue(
            self,
            f"AlbanyHealthStep-DLQ-{env_suffix}",
            queue_name=f"AlbanyHealthStep-DLQ-{env_suffix}",
            retention_period=Duration.days(14),
        )
        
        others_dlq = sqs.Queue(
            self,
            f"AlbanyHealthOthers-DLQ-{env_suffix}",
            queue_name=f"AlbanyHealthOthers-DLQ-{env_suffix}",
            retention_period=Duration.days(14),
        )
        
        heart_rate_dlq = sqs.Queue(
            self,
            f"AlbanyHealthHeartRate-DLQ-{env_suffix}",
            queue_name=f"AlbanyHealthHeartRate-DLQ-{env_suffix}",
            retention_period=Duration.days(14),
        )
        
        processing_files_dlq = sqs.Queue(
            self,
            f"AlbanyHealthSuccessfullProcessingFiles-DLQ-{env_suffix}",
            queue_name=f"AlbanyHealthSuccessfullProcessingFiles-DLQ-{env_suffix}",
            retention_period=Duration.days(14),
        )

        # Configure dead-letter queue settings
        dead_letter_queue = sqs.DeadLetterQueue(
            max_receive_count=max_receive_count,
            queue=main_dlq,
        )

        self.main_queue = sqs.Queue(
            self,
            f"AlbanyHealthMain-SQSQueue-{env_suffix}",
            queue_name=f"AlbanyHealthMain-SQSQueue-{env_suffix}",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=dead_letter_queue,
        )

        self.sleep_queue = sqs.Queue(
            self,
            f"AlbanyHealthSleep-SQSQueue-{env_suffix}",
            queue_name=f"AlbanyHealthSleep-SQSQueue-{env_suffix}",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=sleep_dlq,
            ),
        )

        self.step_queue = sqs.Queue(
            self,
            f"AlbanyHealthStep-SQSQueue-{env_suffix}",
            queue_name=f"AlbanyHealthStep-SQSQueue-{env_suffix}",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=step_dlq,
            ),
        )

        self.others_queue = sqs.Queue(
            self,
            f"AlbanyHealthOthers-SQSQueue-{env_suffix}",
            queue_name=f"AlbanyHealthOthers-SQSQueue-{env_suffix}",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=others_dlq,
            ),
        )

        self.heart_rate_queue = sqs.Queue(
            self,
            f"AlbanyHealthHeartRate-SQSQueue-{env_suffix}",
            queue_name=f"AlbanyHealthHeartRate-SQSQueue-{env_suffix}",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=heart_rate_dlq,
            ),
        )

        self.processing_files_queue = sqs.Queue(
            self,
            f"AlbanyHealthSuccessfullProcessingFiles-SQSQueue-{env_suffix}",
            queue_name=f"AlbanyHealthSuccessfullProcessingFiles-SQSQueue-{env_suffix}",
            visibility_timeout=visibility_timeout,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=processing_files_dlq,
            ),
        )