from typing import Any, Optional

from botocore.exceptions import BotoCoreError, ClientError
from orjson import loads
from pydantic import Field, NonNegativeInt, PositiveInt

from .queue import Message, Queue, QueueConfig


class SQSMessage(Message):
    receipt_handle: str
    attributes: dict[str, Any]
    message_attributes: dict[str, Any]


class SQSQueueConfig(QueueConfig):
    region: str
    url: str
    dlq_url: Optional[str] = None
    max_number_of_messages: PositiveInt = Field(10, le=10)
    visibility_timeout: PositiveInt = Field(30, le=12 * 60 * 60)
    wait_time_seconds: NonNegativeInt = Field(20, le=20)


@Queue.register(SQSQueueConfig)
class SQSQueue(Queue[SQSMessage]):
    config: SQSQueueConfig

    def initialize(self) -> None:
        try:
            from boto3 import client
        except ImportError:
            raise ImportError(
                "Missing SQS dependencies... Install with: pip install eventide[sqs]"
            ) from None

        self.sqs_client = client("sqs", region_name=self.config.region)

        if not self.config.dlq_url:
            try:
                self.config.dlq_url = self._get_dlq_url()
            except (BotoCoreError, RuntimeError):
                pass

    @property
    def max_messages_per_pull(self) -> int:
        return self.config.max_number_of_messages

    def pull_messages(self) -> list[SQSMessage]:
        response = self.sqs_client.receive_message(
            QueueUrl=self.config.url,
            MaxNumberOfMessages=self.max_messages_per_pull,
            WaitTimeSeconds=self.config.wait_time_seconds,
            VisibilityTimeout=self.config.visibility_timeout,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            MessageSystemAttributeNames=["All"],
        )

        return [
            SQSMessage(
                id=message["MessageId"],
                body=self.load_message_body(message["Body"]),
                receipt_handle=message["ReceiptHandle"],
                attributes=message["Attributes"],
                message_attributes=message.get("MessageAttributes") or {},
                eventide_attempt=int(
                    message["Attributes"].get("ApproximateReceiveCount", 1)
                ),
            )
            for message in response.get("Messages") or []
        ]

    def send_message(self, body: Any, *args: Any, **kwargs: Any) -> None:
        self.sqs_client.send_message(
            QueueUrl=self.config.url,
            MessageBody=self.dump_message_body(body),
            MessageAttributes=kwargs.pop("message_attributes", {}),
            DelaySeconds=min(kwargs.pop("delay_seconds", 0), 900),
        )

    def retry_message(self, message: SQSMessage, backoff: int = 0) -> None:
        try:
            self.sqs_client.change_message_visibility(
                QueueUrl=self.config.url,
                ReceiptHandle=message.receipt_handle,
                VisibilityTimeout=backoff or 0,
            )
        except ClientError as exception:
            if exception.response["Error"]["Code"] != "ReceiptHandleIsInvalid":
                raise exception from None

    def ack_message(self, message: SQSMessage) -> None:
        self.sqs_client.delete_message(
            QueueUrl=self.config.url,
            ReceiptHandle=message.receipt_handle,
        )

    def dlq_message(self, message: SQSMessage) -> None:
        if not self.config.dlq_url:
            return

        self.sqs_client.send_message(
            QueueUrl=self.config.dlq_url,
            MessageBody=self.dump_message_body(message.body),
            MessageAttributes=message.message_attributes,
        )
        self.ack_message(message)

    def _get_dlq_url(self) -> str:
        redrive_policy = self.sqs_client.get_queue_attributes(
            QueueUrl=self.config.url,
            AttributeNames=["All"],
        )["Attributes"].get("RedrivePolicy")

        if not redrive_policy:
            raise RuntimeError("No DLQ configured for this queue")

        dlq_arn = loads(redrive_policy).get("deadLetterTargetArn")
        if not dlq_arn:
            raise RuntimeError("No DLQ configured for this queue")

        dlq_arn_parts = dlq_arn.split(":")

        return str(
            self.sqs_client.get_queue_url(
                QueueName=dlq_arn_parts[-1],
                QueueOwnerAWSAccountId=dlq_arn_parts[-2],
            )["QueueUrl"]
        )
