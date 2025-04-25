from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from .matcher import HandlerMatcher

if TYPE_CHECKING:
    from .._queues import Message


@runtime_checkable
class Handler(Protocol):
    name: str
    matcher: HandlerMatcher
    timeout: float
    retry_for: list[type[Exception]]
    retry_limit: int
    retry_min_backoff: float
    retry_max_backoff: float

    def __call__(self, message: "Message") -> Any: ...
