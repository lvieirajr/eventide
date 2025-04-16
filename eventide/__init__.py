from ._handlers import eventide_handler
from ._eventide import Eventide, EventideConfig
from ._queues import CloudflareQueueConfig, MockQueueConfig, SQSQueueConfig
from ._types import Message

__all__ = [
    "CloudflareQueueConfig",
    "Eventide",
    "EventideConfig",
    "Message",
    "MockQueueConfig",
    "SQSQueueConfig",
    "eventide_handler",
]


for name in __all__:
    locals()[name].__module__ = "eventide"
