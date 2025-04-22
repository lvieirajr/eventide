from abc import ABC, abstractmethod
from multiprocessing.context import ForkContext
from multiprocessing.queues import Queue as MultiprocessingQueue
from multiprocessing.sharedctypes import Synchronized
from typing import Any, Callable, ClassVar, Generic, TypeVar

from orjson import JSONDecodeError, loads
from pydantic import Field, NonNegativeInt, PositiveInt

from .._handlers import Handler
from .._utils.pydantic import PydanticModel

TMessage = TypeVar("TMessage", bound="Message")


class MessageMetadata(PydanticModel):
    attempt: PositiveInt = 1
    retry_at: float = Field(None, validate_default=False)  # type: ignore[assignment]
    handler: Handler = Field(None, validate_default=False)  # type: ignore[assignment]


class Message(PydanticModel):
    id: str
    body: Any
    eventide_metadata: MessageMetadata = Field(default_factory=MessageMetadata)


class QueueConfig(PydanticModel):
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
    def max_messages_per_pull(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def pull_messages(self) -> list[TMessage]:
        raise NotImplementedError

    @abstractmethod
    def ack_message(self, message: TMessage) -> None:
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
    def load_message_body(body: str) -> Any:
        try:
            return loads(body)
        except JSONDecodeError:
            return body

    @property
    def empty(self) -> bool:
        with self._size.get_lock():
            return bool(self._size.value == 0)

    @property
    def full(self) -> bool:
        buffer_size = self._config.buffer_size

        if buffer_size == 0:
            return False

        with self._size.get_lock():
            return bool(self._size.value == buffer_size)

    @property
    def should_pull(self) -> bool:
        buffer_size = self._config.buffer_size

        if buffer_size == 0:
            return True

        with self._size.get_lock():
            return bool(buffer_size - self._size.value >= self.max_messages_per_pull)

    def get_message(self) -> TMessage:
        message = self._message_buffer.get_nowait()

        with self._size.get_lock():
            self._size.value -= 1

        return message

    def put_message(self, message: TMessage) -> None:
        with self._size.get_lock():
            self._message_buffer.put_nowait(message)
            self._size.value += 1

    def get_retry_message(self) -> TMessage:
        return self._retry_buffer.get_nowait()

    def put_retry_message(self, message: TMessage) -> None:
        self._retry_buffer.put_nowait(message)

    def shutdown(self) -> None:
        self._message_buffer.close()
        self._message_buffer.cancel_join_thread()

        self._retry_buffer.close()
        self._retry_buffer.cancel_join_thread()
