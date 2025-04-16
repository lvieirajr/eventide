from abc import ABC, abstractmethod
from functools import cached_property
from logging import Logger
from multiprocessing import Value
from time import sleep
from typing import Any, Callable, Generic, TypeVar

from .._types import Message, QueueConfig, InterProcessCommunication
from .._utils.logging import get_logger

TMessage = TypeVar("TMessage", bound=Message)


class Queue(Generic[TMessage], ABC):
    _queue_type_registry: dict[type[QueueConfig], type["Queue[Any]"]] = {}

    _config: QueueConfig
    _ipc: InterProcessCommunication

    def __init__(self, config: QueueConfig, ipc: InterProcessCommunication) -> None:
        self._config = config
        self._ipc = ipc
        self._size = Value("i", 0)

    @cached_property
    def _logger(self) -> Logger:
        return get_logger(name="eventide.queue")

    @property
    def size(self) -> int:
        with self._size.get_lock():
            return self._size.value

    @classmethod
    def register(
        cls,
        queue_config_type: type[QueueConfig],
    ) -> Callable[[type["Queue[Any]"]], type["Queue[Any]"]]:
        def inner(queue_subclass: type[Queue[Any]]) -> type[Queue[Any]]:
            cls._queue_type_registry[queue_config_type] = queue_subclass
            return queue_subclass

        return inner

    @classmethod
    def factory(
        cls,
        config: QueueConfig,
        ipc: InterProcessCommunication,
    ) -> "Queue[Any]":
        queue_subclass = cls._queue_type_registry.get(type(config))

        if not queue_subclass:
            raise ValueError(
                f"No queue implementation found for {type(config).__name__}",
            )

        return queue_subclass(config=config, ipc=ipc)

    def put(self, message: TMessage) -> None:
        while True:
            with self._size.get_lock():
                if (
                    not self._config.buffer_size
                    or self._size.value < self._config.buffer_size
                ):
                    self._ipc.buffer.put(message)
                    self._size.value += 1
                    return

            sleep(0.1)

    def get(self) -> TMessage:
        message = self._ipc.buffer.get_nowait()

        with self._size.get_lock():
            self._size.value -= 1

        return message

    @abstractmethod
    def pull_messages(self) -> list[TMessage]:
        raise NotImplementedError

    @abstractmethod
    def ack_messages(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def dlq_messages(self) -> None:
        raise NotImplementedError
