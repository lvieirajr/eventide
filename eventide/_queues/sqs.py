from functools import cached_property
from logging import Logger
from json import loads
from sys import maxsize

from pydantic import Field, NonNegativeInt, PositiveInt

from .queue import Queue
from .._types import Message, QueueConfig, StrAnyDictType
from .._utils.logging import get_logger


class SQSMessage(Message):
    receipt_handle: str
    attributes: StrAnyDictType
    message_attributes: StrAnyDictType


class SQSQueueConfig(QueueConfig):
    region: str
    url: str
    dlq_url: str | None = None
    visibility_timeout: PositiveInt = Field(30, le=12 * 60 * 60)
    max_number_of_messages: PositiveInt = Field(10, le=10)
    wait_time_seconds: NonNegativeInt = Field(20, le=20)


@Queue.register(SQSQueueConfig)
class SQSQueue(Queue[SQSMessage]):
    _config: SQSQueueConfig

    def __init__(self, config: SQSQueueConfig) -> None:
        try:
            from boto3 import client
        except ImportError:
            raise ImportError("Install boto3 to use SQSQueue")

        super().__init__(config=config)

        self._sqs_client = client("sqs", region_name=self._config.region)

    @cached_property
    def _logger(self) -> Logger:
        return get_logger(name="sqs", parent=super()._logger)

    def pull_messages(self) -> list[SQSMessage]:
        with self._size.get_lock():
            max_messages = min(
                self._config.max_number_of_messages,
                (self._config.buffer_size or maxsize) - self._size.value,
            )

        response = self._sqs_client.receive_message(
            QueueUrl=self._config.url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=self._config.wait_time_seconds,
            VisibilityTimeout=self._config.visibility_timeout,
            MessageSystemAttributeNames=["All"],
        )
        messages = response.get("Messages", [])

        self._logger.info(
            f"Pulled {len(messages)} messages from SQS Queue",
            extra={
                "config": self._config.model_dump(logging=True),
                "messages": len(messages),
            },
        )

        return [
            SQSMessage(
                id=message["MessageId"],
                body=loads(message.get("Body", "{}"), strict=False),
                receipt_handle=message["ReceiptHandle"],
                attributes=message["Attributes"],
                message_attributes=message["MessageAttributes"],
            )
            for message in messages
        ]

    def ack_message(self, message: SQSMessage) -> None:
        self._sqs_client.delete_message(
            QueueUrl=self._config.url,
            ReceiptHandle=message.receipt_handle,
        )

    def dlq_message(self, message: SQSMessage) -> None:
        if self._config.dlq_url:
            self._sqs_client.send_message(
                QueueUrl=self._config.dlq_url,
                MessageBody=message.body,
                MessageAttributes=message.message_attributes,
            )
            return

        self._logger.warning(
            "No DLQ configured",
            extra={
                "config": self._config.model_dump(logging=True),
                "message": message.id,
            },
        )
