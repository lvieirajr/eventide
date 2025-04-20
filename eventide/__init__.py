from ._eventide import Eventide, EventideConfig
from ._exceptions import (
    EventideError,
    WorkerCrashedError,
    WorkerError,
    WorkerTimeoutError,
)
from ._handlers import eventide_handler
from ._queues import (
    CloudflareQueueConfig,
    Message,
    MockQueueConfig,
    Queue,
    QueueConfig,
    SQSQueueConfig,
)

__all__ = [
    "CloudflareQueueConfig",
    "Eventide",
    "EventideConfig",
    "EventideError",
    "Message",
    "MockQueueConfig",
    "Queue",
    "QueueConfig",
    "SQSQueueConfig",
    "WorkerCrashedError",
    "WorkerError",
    "WorkerTimeoutError",
    "eventide_handler",
]


for name in __all__:
    locals()[name].__module__ = "eventide"
