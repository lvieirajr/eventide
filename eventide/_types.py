from multiprocessing.managers import EventProxy  # type: ignore [attr-defined]
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


class MessageState(BaseModel):
    """
    Internal state maintained by Eventide for message processing.

    Attributes:
        retry_count (int): The number of times this message has been retried.
        retry_for_handlers (Set[str]): Set of handler names that need to be retried.
        next_retry_time (Optional[float]): Timestamp for when this message should be retried.
        last_exception (Optional[str]): String representation of the last exception that occurred.
    """

    retry_count: int = 0
    retry_for_handlers: set[str] = Field(default_factory=set)
    next_retry_time: Optional[float] = None
    last_exception: Optional[str] = None


class Message(BaseModel):
    """
    A basic queue message.

    Attributes:
        id (str): The unique identifier of the message.
        body (StrAnyDictType): The body of the message.
        state (MessageState): Internal state maintained by Eventide.
    """

    id: str
    body: StrAnyDictType
    state: MessageState = Field(default_factory=MessageState)


HandlerMatcherType = Callable[[Message], bool]
HandlerFuncType = Callable[[Message], Any]


class QueueConfig(BaseModel):
    """
    Configuration for a queue.

    Attributes:
        name (str): The name of the queue.
        size (NonNegativeInt): The maximum size of the internal queue buffer.
        min_poll_interval (PositiveFloat): The minimum time to wait between polls.
        max_poll_interval (PositiveFloat): The maximum time to wait between polls.
    """

    name: str = Field(default_factory=lambda: uuid4().hex[:8])
    size: NonNegativeInt = 0
    min_poll_interval: PositiveFloat = 1.0
    max_poll_interval: PositiveFloat = 10.0


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
        retry_jitter (float): Random jitter factor (0.0-1.0) to add to backoff times.
        retry_limit (NonNegativeInt): Maximum number of retry attempts.
    """

    retry: Union[bool, list[Type[Exception]]] = False
    retry_on_timeout: bool = False
    retry_min_backoff: PositiveFloat = 1.0
    retry_max_backoff: PositiveFloat = 60.0
    retry_backoff_multiplier: PositiveFloat = 2.0
    retry_jitter: float = Field(0.1, ge=0.0, le=1.0)
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
        shutdown (Event): The event used to signal the shutdown of the application.
        buffer_pairs (list[tuple[Queue[Any], Queue[Any]]]): The pairs of queues used to
            communicate between the queues and workers. The first queue of the pair
            contains the messages pending handling and the second one contains the
            messages pending acknowledgement.
        retry_buffer (Queue[Any]): A queue containing messages that need to be retried.
        handlers (set[tuple[HandlerMatcherType, AnyCallableType]]): The set of all the
            handler matchers and handler function pairs.
    """

    shutdown: EventProxy
    buffer_pairs: list[tuple[Queue[Any], Queue[Any]]]
    retry_buffer: Queue[Any]
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
