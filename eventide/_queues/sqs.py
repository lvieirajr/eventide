from multiprocessing.context import ForkContext
from typing import Any, Optional

from pydantic import Field, PositiveInt

from .queue import Message, Queue, QueueConfig


class SQSMessage(Message):
    receipt_handle: str
    attributes: dict[str, Any]
    message_attributes: dict[str, Any]


class SQSQueueConfig(QueueConfig):
    region: str
    url: str
    dlq_url: Optional[str] = None
    visibility_timeout: PositiveInt = Field(30, le=12 * 60 * 60)
    max_number_of_messages: PositiveInt = Field(10, le=10)


@Queue.register(SQSQueueConfig)
class SQSQueue(Queue[SQSMessage]):
    _config: SQSQueueConfig

    def __init__(self, config: SQSQueueConfig, context: ForkContext) -> None:
        try:
            from boto3 import client
        except ImportError:
            raise ImportError("Install boto3 to use SQSQueue") from None

        super().__init__(config=config, context=context)

        self._sqs_client = client("sqs", region_name=self._config.region)

    @property
    def max_messages_per_poll(self) -> int:
        return self._config.max_number_of_messages

    def pull_messages(self) -> list[SQSMessage]:
        response = self._sqs_client.receive_message(
            QueueUrl=self._config.url,
            MaxNumberOfMessages=self.max_messages_per_poll,
            WaitTimeSeconds=1,
            VisibilityTimeout=self._config.visibility_timeout,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            MessageSystemAttributeNames=["All"],
        )

        return [
            SQSMessage(
                id=message["MessageId"],
                body=self.parse_message_body(raw_body=message["Body"]),
                receipt_handle=message["ReceiptHandle"],
                attributes=message["Attributes"],
                message_attributes=message.get("MessageAttributes") or {},
            )
            for message in response.get("Messages") or []
        ]

    def ack_message(self, message: SQSMessage) -> None:
        self._sqs_client.delete_message(
            QueueUrl=self._config.url,
            ReceiptHandle=message.receipt_handle,
        )

    def dlq_message(self, message: SQSMessage) -> None:
        if not self._config.dlq_url:
            return

        self._sqs_client.send_message(
            QueueUrl=self._config.dlq_url,
            MessageBody=message.body,
            MessageAttributes=message.message_attributes,
        )
