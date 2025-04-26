from collections.abc import Generator
from contextlib import contextmanager
from logging import getLogger
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
    config: CloudflareQueueConfig

    @property
    def max_messages_per_pull(self) -> int:
        return self.config.batch_size

    def initialize(self) -> None:
        try:
            from cloudflare import Cloudflare
        except ImportError:
            raise ImportError(
                "Missing cloudflare dependencies... Install with: pip install "
                "eventide[cloudflare]"
            ) from None

        self.cloudflare_client = Cloudflare()

    def send_message(self, body: Any) -> None:
        # TODO: Seems like cloudflare does not support sending messages at the moment
        # with their SDK
        pass

    def pull_messages(self) -> list[CloudflareMessage]:
        with self._suppress_httpx_info_logs():
            response = self.cloudflare_client.queues.messages.pull(
                self.config.queue_id,
                account_id=self.config.account_id,
                batch_size=self.max_messages_per_pull,
            )

        return [
            CloudflareMessage(
                id=message["id"],
                body=self.load_message_body(message["body"]),
                lease_id=message["lease_id"],
                metadata=message["metadata"],
                timestamp_ms=message["timestamp_ms"],
                attempts=int(message["attempts"]),
            )
            for message in response.result["messages"]  # type: ignore[call-overload]
        ]

    def ack_message(self, message: CloudflareMessage) -> None:
        with self._suppress_httpx_info_logs():
            self.cloudflare_client.queues.messages.ack(
                self.config.queue_id,
                account_id=self.config.account_id,
                acks=[{"lease_id": message.lease_id}],
            )

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
