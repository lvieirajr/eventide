from json import loads
from sys import maxsize

from pydantic import Field, NonNegativeInt, PositiveInt

from .queue import Queue
from .._types import InterProcessCommunication, Message, QueueConfig, StrAnyDictType


class SQSMessage(Message):
    receipt_handle: str
    attributes: StrAnyDictType
    message_attributes: StrAnyDictType


class SQSQueueConfig(QueueConfig):
    url: str
    region: str
    visibility_timeout: PositiveInt = Field(30, le=12 * 60 * 60)
    max_number_of_messages: PositiveInt = Field(10, le=10)
    wait_time_seconds: NonNegativeInt = Field(20, le=20)


@Queue.register(SQSQueueConfig)
class SQSQueue(Queue[SQSMessage]):
    _config: SQSQueueConfig

    def __init__(
        self,
        config: SQSQueueConfig,
        ipc: InterProcessCommunication,
    ) -> None:
        try:
            from boto3 import client
        except ImportError:
            raise ImportError("Install boto3 to use SQSQueue")

        super().__init__(config=config, ipc=ipc)

        self._sqs_client = client("sqs", region_name=self._config.region)

    def pull_messages(self) -> int:
        max_messages = min(
            self._config.max_number_of_messages,
            (
                (self._config.max_buffer_size or maxsize)
                - self._sync_data.message_queue.qsize()
            ),
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
