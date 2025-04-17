from logging import INFO
from multiprocessing.context import ForkProcess
from typing import Any, Callable, Literal, Optional, Type

from pydantic import (
    BaseModel as PydanticBaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
)

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
    handler: Optional[Callable[..., Any]] = None
    attempt: PositiveInt = 1
    retry_at: Optional[float] = None


class Message(BaseModel):
    id: str
    body: StrAnyDictType

    eventide_metadata: MessageMetadata = Field(default_factory=MessageMetadata)


class QueueConfig(BaseModel):
    buffer_size: NonNegativeInt = 0


class HeartBeat(BaseModel):
    worker_id: int
    timestamp: float
    message: Optional[Message] = None


class WorkerState(BaseModel):
    worker_id: int
    process: ForkProcess
    heartbeat: Optional[float] = None
    message: Optional[Message] = None


class EventideConfig(BaseModel):
    handler_paths: set[str] = Field(default_factory=lambda: {"."})

    queue: QueueConfig

    concurrency: PositiveInt = 1

    timeout: PositiveFloat = 60.0

    retry_for: list[Type[Exception]] = Field(default_factory=list)
    retry_min_backoff: PositiveFloat = 1.0
    retry_max_backoff: PositiveFloat = 60.0
    retry_limit: NonNegativeInt = 3

    log_level: Literal[0, 10, 20, 30, 40, 50] = INFO
