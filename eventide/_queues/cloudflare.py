from json import loads
from sys import maxsize
from typing import Optional, cast

from pydantic import Field, PositiveInt

from .queue import Queue
from .._types import Message, QueueConfig, StrAnyDictType, SyncData


class CloudflareMessage(Message):
    """
    A message from a Cloudflare queue.

    Attributes:
        lease_id (str): The lease ID of the message.
        metadata (StrAnyDict): The metadata of the message.
        timestamp_ms (Optional[float]): The timestamp of the message in milliseconds.
        attempts (Optional[float]): The number of attempts made to process the message.
    """

    lease_id: str
    metadata: StrAnyDictType
    timestamp_ms: Optional[float] = None
    attempts: Optional[float] = None


class CloudflareQueueConfig(QueueConfig):
    """
    Configuration for a CloudflareQueue.

    Attributes:
        queue_id (str): The ID of the queue.
        account_id (str): The ID of the account.
        batch_size (PositiveInt): The maximum number of messages to pull at once.
        visibility_timeout_ms (PositiveInt): The visibility timeout in milliseconds.

    """

    queue_id: str
    account_id: str
    batch_size: PositiveInt = Field(10, le=100)
    visibility_timeout_ms: PositiveInt = Field(30000, le=12 * 60 * 60 * 1000)  # 12h max


@Queue.register(CloudflareQueueConfig)
class CloudflareQueue(Queue[CloudflareMessage]):
    """
    Cloudflare queue implementation.
    """

    _config: CloudflareQueueConfig

    def __init__(self, config: CloudflareQueueConfig, sync_data: SyncData) -> None:
        try:
            from cloudflare import Cloudflare
        except ImportError:
            raise ImportError("Install cloudflare to use CloudflareQueue")

        if not config.name:
            config.name = self._config.queue_id

        super().__init__(config=config, sync_data=sync_data)

        self._cloudflare_client = Cloudflare()

    def pull_messages(self) -> int:
        max_messages = min(
            self._config.batch_size,
            (self._config.size or maxsize) - self.buffer.qsize(),
        )

        messages = self._cloudflare_client.queues.messages.pull(
            self._config.queue_id,
            account_id=self._config.account_id,
            batch_size=max_messages,
        ).result

        self._logger.info(
            f"Pulled {len(messages)} messages from {self}",
            extra={**self._config.model_dump(logging=True), "messages": len(messages)},
        )

        for message in messages:
            self.put(
                CloudflareMessage(
                    id=message.id or "",
                    body=loads(message.body or "{}", strict=False),
                    lease_id=message.lease_id or "",
                    metadata=cast(StrAnyDictType, message.metadata or {}),
                    timestamp_ms=message.timestamp_ms,
                    attempts=message.attempts,
                )
            )

        return len(messages)

    def ack_messages(self, messages: list[CloudflareMessage]) -> None:
        for message in messages:
            self._cloudflare_client.queues.messages.ack(
                self._config.queue_id,
                account_id=self._config.account_id,
                acks=[{"lease_id": message.lease_id}],
            )
