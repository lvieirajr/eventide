from typing import Any

from pydantic import Field, PositiveInt

from .queue import Message, Queue, QueueConfig


class SQSMessage(Message):
    receipt_handle: str
    attributes: dict[str, Any]
    message_attributes: dict[str, Any]


class SQSQueueConfig(QueueConfig):
    region: str
    url: str
    max_number_of_messages: PositiveInt = Field(10, le=10)
    visibility_timeout: PositiveInt = Field(30, le=12 * 60 * 60)


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

    @property
    def max_messages_per_pull(self) -> int:
        return self.config.max_number_of_messages

    def send_message(self, body: Any) -> None:
        self.sqs_client.send_message(
            QueueUrl=self.config.url,
            MessageBody=self.dump_message_body(body),
        )

    def pull_messages(self) -> list[SQSMessage]:
        response = self.sqs_client.receive_message(
            QueueUrl=self.config.url,
            MaxNumberOfMessages=self.max_messages_per_pull,
            WaitTimeSeconds=1,
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
            )
            for message in response.get("Messages") or []
        ]

    def ack_message(self, message: SQSMessage) -> None:
        self.sqs_client.delete_message(
            QueueUrl=self.config.url,
            ReceiptHandle=message.receipt_handle,
        )
