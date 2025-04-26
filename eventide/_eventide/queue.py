from multiprocessing.context import ForkContext
from queue import Empty
from time import time

from .._queues import Message, Queue
from .._utils.logging import eventide_logger
from .config import EventideConfig
from .handler import HandlerManager


class QueueManager:
    config: EventideConfig
    context: ForkContext

    handler_manager: HandlerManager

    queue: Queue[Message]

    _empty_pulls: int

    def __init__(
        self,
        config: EventideConfig,
        context: ForkContext,
        handler_manager: HandlerManager,
    ) -> None:
        self.config = config
        self.context = context

        self.handler_manager = handler_manager

    @property
    def pull_interval(self) -> float:
        return float(
            min(
                self.config.max_pull_interval,
                self.config.min_pull_interval * (2**self._empty_pulls),
            )
        )

    def start(self) -> None:
        self.queue = Queue.factory(config=self.config.queue, context=self.context)
        self._empty_pulls = 0

    def shutdown(self) -> None:
        if hasattr(self, "queue"):
            self.queue.shutdown()

    def enqueue_retries(self) -> None:
        messages = []

        while True:
            try:
                messages.append(self.queue.get_retry_message())
            except Empty:
                break

        for message in sorted(messages, key=lambda m: m.eventide_metadata.retry_at):
            if message.eventide_metadata.retry_at <= time() and not self.queue.full:
                self.queue.put_message(message)
            else:
                self.queue.put_retry_message(message)

    def enqueue_messages(self) -> None:
        if self.queue.should_pull:
            for message in self.queue.pull_messages():
                for handler in self.handler_manager.handlers:
                    if handler.matcher(message):
                        message.eventide_metadata.handler = handler
                        self.queue.put_message(message)
                        break

                if not message.eventide_metadata.handler:
                    eventide_logger.error(
                        f"No handler found for message {message.id}",
                        extra={"message_id": message.id},
                    )

        if self.queue.empty:
            self._empty_pulls += 1
        else:
            self._empty_pulls = 0
