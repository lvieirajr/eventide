from json import loads
from sys import maxsize

from pydantic import NonNegativeInt, PositiveInt

from .queue import Queue
from .._types import Message, QueueConfig, StrAnyDictType, SyncData


class SQSMessage(Message):
    """
    A message from an SQS queue.

    Attributes:
        receipt_handle (str): The receipt handle of the message.
        attributes (StrAnyDictType): The SQS system attributes of the message.
        message_attributes (StrAnyDictType): The attributes of the message.
    """

    receipt_handle: str
    attributes: StrAnyDictType
    message_attributes: StrAnyDictType


class SQSQueueConfig(QueueConfig):
    """
    Configuration for an SQS queue.

    Attributes:
        url (str): The URL of the SQS queue.
        region (str): The AWS region where the SQS queue is located.
        visibility_timeout (PositiveInt): The visibility timeout for messages.
        max_number_of_messages (PositiveInt): The maximum number of messages to pull at
            a time.
        wait_time_seconds (NonNegativeIn): The time to wait in between each message
            pulling iteration (long polling).
    """

    url: str
    region: str
    visibility_timeout: PositiveInt = 30
    max_number_of_messages: PositiveInt = 10
    wait_time_seconds: NonNegativeInt = 20


@Queue.register(SQSQueueConfig)
class SQSQueue(Queue[SQSMessage]):
    """
    SQS queue implementation.
    """

    _config: SQSQueueConfig

    def __init__(self, config: SQSQueueConfig, sync_data: SyncData) -> None:
        try:
            from boto3 import client
        except ImportError:
            raise ImportError("Install boto3 to use SQSQueue")

        if not config.name:
            config.name = config.url.split("/")[-1]

        super().__init__(config=config, sync_data=sync_data)

        self._sqs_client = client("sqs", region_name=self._config.region)

    def pull_messages(self) -> int:
        max_messages = min(
            self._config.max_number_of_messages,
            (self._config.size or maxsize) - self.buffer.qsize(),
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
            f"Pulled {len(messages)} messages from {self}",
            extra={**self._config.model_dump(logging=True), "messages": len(messages)},
        )

        for message in messages:
            self.put(
                SQSMessage(
                    id=message["MessageId"],
                    body=loads(message.get("Body", "{}"), strict=False),
                    receipt_handle=message["ReceiptHandle"],
                    attributes=message["Attributes"],
                    message_attributes=message["MessageAttributes"],
                ),
            )

        return len(messages)

    def ack_messages(self, messages: list[SQSMessage]) -> None:
        for message in messages:
            self._sqs_client.delete_message(
                QueueUrl=self._config.url,
                ReceiptHandle=message.receipt_handle,
            )
