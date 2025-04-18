from collections.abc import Generator
from contextlib import contextmanager
from logging import getLogger
from multiprocessing.context import ForkContext
from sys import maxsize
from typing import Any, Optional

from pydantic import Field, PositiveInt

from .queue import Message, Queue, QueueConfig


class CloudflareMessage(Message):
    lease_id: str
    metadata: dict[str, Any]
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
            raise ImportError("Install cloudflare to use CloudflareQueue") from None

        super().__init__(config=config, context=context)

        self._cloudflare_client = Cloudflare()

    def pull_messages(self) -> list[CloudflareMessage]:
        with self._size.get_lock():
            max_messages = min(
                self._config.batch_size,
                (self._config.buffer_size or maxsize) - self._size.value,
            )

        with self._suppress_httpx_info_logs():
            response = self._cloudflare_client.queues.messages.pull(
                self._config.queue_id,
                account_id=self._config.account_id,
                batch_size=max_messages,
            )

        return [
            CloudflareMessage(
                id=message["id"],
                body=self.parse_message_body(raw_body=message["body"]),
                lease_id=message["lease_id"],
                metadata=message["metadata"],
                timestamp_ms=message["timestamp_ms"],
                attempts=int(message["attempts"]),
            )
            for message in response.result["messages"]  # type: ignore[call-overload]
        ]

    def ack_message(self, message: CloudflareMessage) -> None:
        with self._suppress_httpx_info_logs():
            self._cloudflare_client.queues.messages.ack(
                self._config.queue_id,
                account_id=self._config.account_id,
                acks=[{"lease_id": message.lease_id}],
            )

    def dlq_message(self, message: CloudflareMessage) -> None:
        pass

    @contextmanager
    def _suppress_httpx_info_logs(self) -> Generator[None, None, None]:
        httpx_logger = getLogger("httpx")
        httpcore_logger = getLogger("httpcore")
        httpx_level = httpx_logger.level
        httpcore_level = httpcore_logger.level

        httpx_logger.setLevel("WARNING")
        httpcore_logger.setLevel("WARNING")

        try:
            yield
        finally:
            httpx_logger.setLevel(httpx_level)
            httpcore_logger.setLevel(httpcore_level)
