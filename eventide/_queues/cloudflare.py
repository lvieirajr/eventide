from json import loads
from multiprocessing.context import ForkContext
from sys import maxsize
from typing import Optional, cast

from pydantic import Field, PositiveInt

from .queue import Queue
from .._types import Message, QueueConfig, StrAnyDictType


class CloudflareMessage(Message):
    lease_id: str
    metadata: StrAnyDictType
    timestamp_ms: Optional[float] = None
    attempts: Optional[float] = None


class CloudflareQueueConfig(QueueConfig):
    account_id: str
    queue_id: str
    batch_size: PositiveInt = Field(10, le=100)
    visibility_timeout_ms: PositiveInt = Field(30000, le=12 * 60 * 60 * 1000)


@Queue.register(CloudflareQueueConfig)
class CloudflareQueue(Queue[CloudflareMessage]):
    _config: CloudflareQueueConfig

    def __init__(self, config: CloudflareQueueConfig, context: ForkContext) -> None:
        try:
            from cloudflare import Cloudflare
        except ImportError:
            raise ImportError("Install cloudflare to use CloudflareQueue")

        super().__init__(config=config, context=context)

        self._cloudflare_client = Cloudflare()

    def pull_messages(self) -> list[CloudflareMessage]:
        with self._size.get_lock():
            max_messages = min(
                self._config.batch_size,
                (self._config.buffer_size or maxsize) - self._size.value,
            )

        messages = self._cloudflare_client.queues.messages.pull(
            self._config.queue_id,
            account_id=self._config.account_id,
            batch_size=max_messages,
        ).result

        self._logger.info(
            f"Pulled {len(messages)} messages from Cloudflare Queue",
            extra={
                "config": self._config.model_dump(logging=True),
                "messages": len(messages),
            },
        )

        return [
            CloudflareMessage(
                id=message.id or "",
                body=loads(message.body or "{}", strict=False),
                lease_id=message.lease_id or "",
                metadata=cast(StrAnyDictType, message.metadata or {}),
                timestamp_ms=message.timestamp_ms,
                attempts=message.attempts,
            )
            for message in messages
        ]

    def ack_message(self, message: CloudflareMessage) -> None:
        self._cloudflare_client.queues.messages.ack(
            self._config.queue_id,
            account_id=self._config.account_id,
            acks=[{"lease_id": message.lease_id}],
        )

    def dlq_message(self, message: CloudflareMessage) -> None:
        pass
