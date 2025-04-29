from multiprocessing.context import ForkContext

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
        self.queue = Queue.factory(
            queue_id=1,
            config=self.config.queue,
            context=self.context,
        )
        self._empty_pulls = 0

    def shutdown(self) -> None:
        if hasattr(self, "queue"):
            self.queue.shutdown()

    def enqueue_messages(self) -> None:
        if self.queue.should_pull:
            for message in self.queue.pull_messages():
                matched_handlers = []

                for handler in self.handler_manager.handlers:
                    if handler.matcher(message):
                        matched_handlers.append(handler)

                if len(matched_handlers) == 1:
                    message.eventide_handler = matched_handlers[0]
                    self.queue.put_message(message)
                elif len(matched_handlers) == 0:
                    eventide_logger.error(
                        f"No handler found for message {message.id}",
                        extra={"message_id": message.id},
                    )
                else:
                    eventide_logger.error(
                        f"Multiple handlers found for message {message.id}",
                        extra={
                            "message_id": message.id,
                            "handlers": [handler.name for handler in matched_handlers],
                        },
                    )

            self._empty_pulls = (self._empty_pulls + 1) if self.queue.empty else 0
