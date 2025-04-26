from abc import ABC, abstractmethod
from multiprocessing.context import ForkContext
from multiprocessing.queues import Queue as MultiprocessingQueue
from multiprocessing.sharedctypes import Synchronized
from typing import Any, Callable, ClassVar, Generic, TypeVar

from orjson import JSONDecodeError, JSONEncodeError, dumps, loads
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
    queue_type_registry: ClassVar[dict[type[QueueConfig], type["Queue[Any]"]]] = {}

    config: QueueConfig
    context: ForkContext

    message_buffer: MultiprocessingQueue[TMessage]
    retry_buffer: MultiprocessingQueue[TMessage]

    _size: Synchronized  # type: ignore[type-arg]

    def __init__(self, config: QueueConfig, context: ForkContext) -> None:
        self.config = config
        self.context = context

        self.message_buffer = self.context.Queue(maxsize=self.config.buffer_size)
        self.retry_buffer = self.context.Queue()

        self._size = self.context.Value("i", 0)

        self.initialize()

    @property
    @abstractmethod
    def max_messages_per_pull(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def initialize(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def send_message(self, body: Any) -> None:
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
            cls.queue_type_registry[queue_config_type] = queue_subclass
            return queue_subclass

        return inner

    @classmethod
    def factory(cls, config: QueueConfig, context: ForkContext) -> "Queue[Any]":
        queue_subclass = cls.queue_type_registry.get(type(config))

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

    @staticmethod
    def dump_message_body(body: Any) -> str:
        try:
            return dumps(body).decode("utf-8")
        except JSONEncodeError:
            return str(body)

    @property
    def empty(self) -> bool:
        with self._size.get_lock():
            return bool(self._size.value == 0)

    @property
    def full(self) -> bool:
        buffer_size = self.config.buffer_size

        if buffer_size == 0:
            return False

        with self._size.get_lock():
            return bool(self._size.value == buffer_size)

    @property
    def should_pull(self) -> bool:
        buffer_size = self.config.buffer_size

        if buffer_size == 0:
            return True

        with self._size.get_lock():
            return bool(buffer_size - self._size.value >= self.max_messages_per_pull)

    def get_message(self) -> TMessage:
        message = self.message_buffer.get_nowait()

        with self._size.get_lock():
            self._size.value -= 1

        return message

    def put_message(self, message: TMessage) -> None:
        with self._size.get_lock():
            self.message_buffer.put_nowait(message)
            self._size.value += 1

    def get_retry_message(self) -> TMessage:
        return self.retry_buffer.get_nowait()

    def put_retry_message(self, message: TMessage) -> None:
        self.retry_buffer.put_nowait(message)

    def shutdown(self) -> None:
        self.message_buffer.close()
        self.message_buffer.cancel_join_thread()

        self.retry_buffer.close()
        self.retry_buffer.cancel_join_thread()
