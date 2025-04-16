from json import loads
from sys import maxsize
from typing import Optional, cast

from pydantic import Field, PositiveInt

from .queue import Queue
from .._types import InterProcessCommunication, Message, QueueConfig, StrAnyDictType


class CloudflareMessage(Message):
    lease_id: str
    metadata: StrAnyDictType
    timestamp_ms: Optional[float] = None
    attempts: Optional[float] = None


class CloudflareQueueConfig(QueueConfig):
    queue_id: str
    account_id: str
    batch_size: PositiveInt = Field(10, le=100)
    visibility_timeout_ms: PositiveInt = Field(30000, le=12 * 60 * 60 * 1000)


@Queue.register(CloudflareQueueConfig)
class CloudflareQueue(Queue[CloudflareMessage]):
    _config: CloudflareQueueConfig

    def __init__(
        self,
        config: CloudflareQueueConfig,
        ipc: InterProcessCommunication,
    ) -> None:
        try:
            from cloudflare import Cloudflare
        except ImportError:
            raise ImportError("Install cloudflare to use CloudflareQueue")

        super().__init__(config=config, ipc=ipc)

        self._cloudflare_client = Cloudflare()

    def pull_messages(self) -> int:
        max_messages = min(
            self._config.batch_size,
            (
                (self._config.max_buffer_size or maxsize)
                - self._sync_data.message_queue.qsize()
            ),
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
