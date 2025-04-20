from abc import ABC, abstractmethod
from json import JSONDecodeError, loads
from multiprocessing.context import ForkContext
from multiprocessing.queues import Queue as MultiprocessingQueue
from multiprocessing.sharedctypes import Synchronized
from queue import Empty
from sys import maxsize
from time import time
from typing import Any, Callable, ClassVar, Generic, TypeVar

from pydantic import Field, NonNegativeInt, PositiveInt

from .._handlers import Handler, handler_registry
from .._utils import BaseModel, queue_logger

TMessage = TypeVar("TMessage", bound="Message")


class MessageMetadata(BaseModel):
    attempt: PositiveInt = 1
    retry_at: float = Field(None, validate_default=False)  # type: ignore[assignment]
    handler: Handler = Field(None, validate_default=False)  # type: ignore[assignment]


class Message(BaseModel):
    id: str
    body: dict[str, Any]
    eventide_metadata: MessageMetadata = Field(default_factory=MessageMetadata)


class QueueConfig(BaseModel):
    buffer_size: NonNegativeInt = 0


class Queue(Generic[TMessage], ABC):
    _queue_type_registry: ClassVar[dict[type[QueueConfig], type["Queue[Any]"]]] = {}

    _config: QueueConfig
    _context: ForkContext

    _message_buffer: MultiprocessingQueue[TMessage]
    _retry_buffer: MultiprocessingQueue[TMessage]

    _size: Synchronized  # type: ignore[type-arg]

    def __init__(self, config: QueueConfig, context: ForkContext) -> None:
        self._config = config
        self._context = context

        self._message_buffer = self._context.Queue(maxsize=self._config.buffer_size)
        self._retry_buffer = self._context.Queue()

        self._size = self._context.Value("i", 0)

    @property
    @abstractmethod
    def max_messages_per_poll(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def pull_messages(self) -> list[TMessage]:
        raise NotImplementedError

    @abstractmethod
    def ack_message(self, message: TMessage) -> None:
        raise NotImplementedError

    @abstractmethod
    def dlq_message(self, message: TMessage) -> None:
        raise NotImplementedError

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
    def factory(cls, config: QueueConfig, context: ForkContext) -> "Queue[Any]":
        queue_subclass = cls._queue_type_registry.get(type(config))

        if not queue_subclass:
            raise ValueError(
                f"No queue implementation found for {type(config).__name__}",
            )

        return queue_subclass(config=config, context=context)

    @staticmethod
    def parse_message_body(raw_body: str) -> dict[str, Any]:
        body = None

        try:
            body = loads(raw_body, strict=False)
        except JSONDecodeError:
            pass

        if not isinstance(body, dict):
            body = {"raw": raw_body}

        return body

    @property
    def empty(self) -> bool:
        with self._size.get_lock():
            return bool(self._size.value == 0)

    def get_message(self) -> TMessage:
        message = self._message_buffer.get_nowait()

        with self._size.get_lock():
            self._size.value -= 1

        return message

    def buffer_retry(self, message: TMessage) -> None:
        self._retry_buffer.put_nowait(message)

    def enqueue_retries(self) -> None:
        messages_to_retry = []

        while True:
            try:
                messages_to_retry.append(self._retry_buffer.get_nowait())
            except Empty:
                break

        messages_to_retry = sorted(
            messages_to_retry,
            key=lambda m: m.eventide_metadata.retry_at,
        )

        now = time()
        for message in messages_to_retry:
            retry_at = message.eventide_metadata.retry_at

            if retry_at <= now:
                with self._size.get_lock():
                    if (
                        self._config.buffer_size == 0
                        or self._size.value < self._config.buffer_size
                    ):
                        self._message_buffer.put_nowait(message)
                        self._size.value += 1
                    else:
                        self.buffer_retry(message=message)
            else:
                self.buffer_retry(message=message)

    def enqueue_messages(self) -> None:
        with self._size.get_lock():
            buffer_size = self._config.buffer_size or maxsize

            if buffer_size - self._size.value < self.max_messages_per_poll:
                return

        for message in self.pull_messages():
            for handler in handler_registry:
                if handler.matcher(message):
                    message.eventide_metadata.handler = handler

                    with self._size.get_lock():
                        self._message_buffer.put_nowait(message)
                        self._size.value += 1
                        break

            if not message.eventide_metadata.handler:
                queue_logger.error(
                    f"No handler found for message {message.id}",
                    extra={"message_id": message.id},
                )
                self.dlq_message(message)

    def shutdown(self) -> None:
        self._message_buffer.close()
        self._message_buffer.cancel_join_thread()

        self._retry_buffer.close()
        self._retry_buffer.cancel_join_thread()
