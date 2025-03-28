from datetime import datetime
from multiprocessing.managers import (  # type: ignore [attr-defined]
    DictProxy,
    EventProxy,
)
from queue import Queue
from typing import Any, Callable, Optional, Union, Type
from uuid import uuid4

from pydantic import (
    BaseModel as PydanticBaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    SkipValidation,
)

AnyCallableType = Callable[..., Any]
StrAnyDictType = dict[str, Any]


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_dump(self, *, logging: bool = False, **kwargs: Any) -> StrAnyDictType:
        dump = super().model_dump(**kwargs)

        if logging:
            for reserved_attr in {"name", "message"}:
                if reserved_attr in dump:
                    dump[f"{reserved_attr}_"] = dump.pop(reserved_attr)

        return dump


class MessageMetadata(BaseModel):
    """
    Internal state maintained by Eventide for message processing.

    Attributes:
        message_queue: Queue[Any]: The internal queue this message was initially pulled
            into.
        ack_queue: Queue[Any]: The internal queue where this message is stored after
            being handled so it can be acknowledged.
        retry_dict: DictProxy[str, Any]: A dictionary with message id and message as key
            value pairs to be retried at some point in the future.
        retry_count (NonNegativeInt): The number of times this message has been retried.
        failed_handlers (Set[str]): Set of handler names that need to be retried.
        next_retry (Optional[datetime]): Time for the next retry to be enqueued.
    """

    message_queue: SkipValidation[Queue[Any]]
    ack_queue: SkipValidation[Queue[Any]]
    retry_dict: DictProxy[str, Any]
    retry_count: NonNegativeInt = 0
    failed_handlers: set[str] = Field(default_factory=set)
    next_retry: Optional[datetime] = None


class Message(BaseModel):
    """
    A basic queue message.

    Attributes:
        id (str): The unique identifier of the message.
        body (StrAnyDictType): The body of the message.
        eventide_metadata (MessageMetadata): Internal metadata maintained by Eventide.
    """

    id: str
    body: StrAnyDictType
    eventide_metadata: MessageMetadata

    def ack(self) -> None:
        self.eventide_metadata.ack_queue.put(self)

    def retry(self) -> None:
        self.eventide_metadata.retry_dict[self.id] = self


HandlerMatcherType = Callable[[Message], bool]
HandlerFuncType = Callable[[Message], Any]


class QueueConfig(BaseModel):
    """
    Configuration for a queue.

    Attributes:
        name (str): The name of the queue.
        size (NonNegativeInt): The maximum size of the internal queue.
    """

    name: str = Field(default_factory=lambda: uuid4().hex[:8])
    size: NonNegativeInt = 0


class RetryConfig(BaseModel):
    """
    Configuration for retry behavior.

    Attributes:
        retry (Union[bool, list[Type[Exception]]]): If True, retry on any exception;
            if a list, retry only on specified exception types.
        retry_on_timeout (bool): If True, retry on timeout.
        retry_min_backoff (PositiveFloat): Minimum backoff time in seconds.
        retry_max_backoff (PositiveFloat): Maximum backoff time in seconds.
        retry_backoff_multiplier (PositiveFloat): Multiplier for exponential backoff.
        retry_limit (NonNegativeInt): Maximum number of retry attempts.
    """

    retry: Union[bool, list[Type[Exception]]] = False
    retry_on_timeout: bool = False
    retry_min_backoff: PositiveFloat = 1.0
    retry_max_backoff: PositiveFloat = 60.0
    retry_backoff_multiplier: PositiveFloat = 2.0
    retry_limit: NonNegativeInt = 3


class WorkerConfig(BaseModel):
    """
    Configuration for a Worker.

    Attributes:
        name (str): The name of the worker.
        timeout (PositiveFloat): The maximum time a handler can run for a given
            message.
        retry_config (RetryConfig): Configuration for retry behavior.
    """

    name: str = Field(default_factory=lambda: uuid4().hex[:8])
    timeout: PositiveFloat = 600.0
    retry_config: RetryConfig = Field(default_factory=RetryConfig)


class SyncData(BaseModel):
    """
    Object used to share the primitives between the several underlying processes and
    threads.

    Attributes:
        shutdown (EventProxy): The event used to signal the shutdown of the application.
        message_queues (list[Queue[Any]]): The internal queues where messages pulled
            from each of the external queues are stored.
        ack_queues (list[Queue[Any]]): The internal queues where messages that have been
            handled and are ready to be acknowledged are stored.
        retry_dicts (list[DictProxy[str, Any]]): The dictionaries containing messages
            that need to be retried from each queue.
        handlers (set[tuple[HandlerMatcherType, AnyCallableType]]): The set of all the
            handler matchers and handler function pairs.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    shutdown: EventProxy
    message_queues: list[SkipValidation[Queue[Any]]]
    ack_queues: list[SkipValidation[Queue[Any]]]
    retry_dicts: list[DictProxy[str, Any]]
    handlers: set[tuple[HandlerMatcherType, AnyCallableType]]


class EventideConfig(BaseModel):
    """
    Configuration for the Eventide application.

    Attributes:
        handler_discovery_paths (set[str]): The directories to search for
            handlers.
        queues (list[QueueConfig]): Configuration for the queues to be used by Eventide.
        workers (Union[list[WorkerConfig], PositiveInt]): Configuration for the
            workers to be used by Eventide.
            If a list of WorkerConfig is provided, each worker will be configured
            individually.
            If a PositiveInt is provided, a number of workers will be created with
            default configurations.
    """

    handler_discovery_paths: set[str] = Field(default_factory=lambda: {"."})

    queues: list[QueueConfig] = Field(default_factory=list)
    workers: Union[list[WorkerConfig], PositiveInt] = 1
