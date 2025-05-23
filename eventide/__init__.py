from ._eventide import Eventide, EventideConfig
from ._exceptions import EventideError, WorkerCrashedError, WorkerError
from ._queues import Message, Queue, QueueConfig, SQSQueueConfig

__all__ = [
    "Eventide",
    "EventideConfig",
    "EventideError",
    "Message",
    "Queue",
    "QueueConfig",
    "SQSQueueConfig",
    "WorkerCrashedError",
    "WorkerError",
]


for name in __all__:
    locals()[name].__module__ = "eventide"
