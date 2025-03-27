from abc import ABC, abstractmethod
from logging import Logger
from queue import Empty
from threading import Thread
from time import sleep
from typing import Any, Callable, Generic, TypeVar

from .._types import BaseModel, Message, QueueConfig, SyncData
from .._utils.logging import get_logger

TMessage = TypeVar("TMessage", bound=Message)


class Queue(Generic[TMessage], ABC):
    """
    Base class for queue implementations.
    This abstract class defines the interface that all queue implementations must
    follow.
    """

    _registry: dict[type[QueueConfig], type["Queue[Any]"]] = {}

    _config: QueueConfig
    _sync_data: SyncData

    _current_poll_interval: float

    def __init__(self, config: QueueConfig, sync_data: SyncData) -> None:
        self._config = config
        self._sync_data = sync_data

        self._current_poll_interval = self._config.min_poll_interval

        self.buffer = self._sync_data.message_buffers[-1]
        self.ack_buffer = self._sync_data.ack_buffers[-1]

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name='{self._config.name}')"

    @abstractmethod
    def pull_messages(self) -> int:
        """
        Pull messages from the external queue and put them into the internal queue.
        This method should be implemented by subclasses to pull messages from
        their specific queue implementation.
        """
        pass

    @abstractmethod
    def ack_messages(self, messages: list[TMessage]) -> None:
        """
        Acknowledge messages that have been processed.
        This method should be implemented by subclasses to acknowledge messages
        that have been processed.
        """
        pass

    @classmethod
    def register(
        cls,
        queue_config_type: type[QueueConfig],
    ) -> Callable[[type["Queue[Any]"]], type["Queue[Any]"]]:
        def inner(queue_subclass: type[Queue[Any]]) -> type[Queue[Any]]:
            cls._registry[queue_config_type] = queue_subclass
            return queue_subclass

        return inner

    @classmethod
    def factory(cls, config: QueueConfig, sync_data: SyncData) -> "Queue[Any]":
        queue_subclass = cls._registry.get(type(config))

        if not queue_subclass:
            raise ValueError(f"No queue implementation found for {type(config)}")

        return queue_subclass(config=config, sync_data=sync_data)

    @property
    def _logger(self) -> Logger:
        return get_logger(name=f"{type(self).__name__}.{self._config.name}")

    def put(self, message: TMessage) -> None:
        self.buffer.put(message, block=True)

    def run(self) -> None:
        self._logger.info(
            f"{self} is starting...",
            extra={**self._config.model_dump(logging=True)},
        )

        while not self._sync_data.shutdown.is_set():
            messages_pulled = (
                self.pull_messages() if self.buffer.qsize() < self._config.size else -1
            )
            self._process_ack_buffer()

            sleep(self._current_poll_interval)

            if messages_pulled == 0:
                self._current_poll_interval = min(
                    self._current_poll_interval * 2,
                    self._config.max_poll_interval,
                )
            elif messages_pulled > 0:
                self._current_poll_interval = self._config.min_poll_interval

        self._logger.info(
            f"{self} stopped",
            extra={**self._config.model_dump(logging=True)},
        )

    def _process_ack_buffer(self) -> int:
        acked_messages = []

        while True:
            try:
                acked_messages.append(self.ack_buffer.get_nowait())
            except Empty:
                break

        if acked_messages:
            self.ack_messages(messages=acked_messages)

        return len(acked_messages)


class RunningQueue(BaseModel):
    """
    State of a running queue.

    Attributes:
        queue (Queue[Any]): The queue object.
        thread (Thread): The thread running the queue.
        config (QueueConfig): Configuration for the queue.
    """

    queue: Queue[Any]

    thread: Thread
    config: QueueConfig
