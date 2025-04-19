from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, cast

from .handler import Handler
from .matcher import HandlerMatcher
from .registry import handler_registry

if TYPE_CHECKING:
    from .._queues import Message


def eventide_handler(
    *matchers: str,
    operator: Literal["all", "any", "and", "or"] = "all",
    timeout: Optional[float] = None,
    retry_for: Optional[list[type[Exception]]] = None,
    retry_limit: Optional[int] = None,
    retry_min_backoff: Optional[float] = None,
    retry_max_backoff: Optional[float] = None,
) -> Callable[..., Any]:
    def decorator(
        func: Callable[["Message"], Any],
    ) -> Handler:
        @wraps(func)
        def wrapper(message: "Message") -> Any:
            return func(message)

        wrapper.name = (  # type: ignore[attr-defined]
            f"{func.__module__}.{func.__qualname__}"
        )
        wrapper.matcher = HandlerMatcher(  # type: ignore[attr-defined]
            *matchers,
            operator=operator,
        )
        wrapper.timeout = timeout  # type: ignore[attr-defined]
        wrapper.retry_for = retry_for  # type: ignore[attr-defined]
        wrapper.retry_limit = retry_limit  # type: ignore[attr-defined]
        wrapper.retry_min_backoff = retry_min_backoff  # type: ignore[attr-defined]
        wrapper.retry_max_backoff = retry_max_backoff  # type: ignore[attr-defined]

        handler_registry.add(cast(Handler, wrapper))

        return cast(Handler, wrapper)

    return decorator
