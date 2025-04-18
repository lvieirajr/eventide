from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Type

from .matcher import HandlerMatcher
from .registry import handler_registry

if TYPE_CHECKING:
    from .._queues import Message


def eventide_handler(
    *matchers: str,
    operator: Literal["all", "any", "and", "or"] = "all",
    timeout: Optional[float] = None,
    retry_for: Optional[list[Type[Exception]]] = None,
    retry_limit: Optional[int] = None,
    retry_min_backoff: Optional[float] = None,
    retry_max_backoff: Optional[float] = None,
) -> Callable[..., Any]:
    def decorator(func: Callable[["Message"], Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(message: "Message") -> Any:
            return func(message)

        wrapper.timeout = timeout
        wrapper.retry_for = retry_for
        wrapper.retry_limit = retry_limit
        wrapper.retry_min_backoff = retry_min_backoff
        wrapper.retry_max_backoff = retry_max_backoff

        handler_registry.add((HandlerMatcher(*matchers, operator=operator), wrapper))

        return wrapper

    return decorator
