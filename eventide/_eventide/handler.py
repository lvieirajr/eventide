from collections.abc import Iterable
from functools import wraps
from typing import Any, Callable, Optional, Union, cast

from .._handlers import Handler, HandlerMatcher, MatcherCallable
from .._queues import Message
from .config import EventideConfig


class HandlerManager:
    config: EventideConfig

    handlers: set[Handler]

    def __init__(self, config: EventideConfig) -> None:
        self.config = config
        self.handlers = set()

    def register(
        self,
        *matchers: Union[str, MatcherCallable],
        operator: Callable[[Iterable[bool]], bool] = all,
        timeout: Optional[float] = None,
        retry_for: Optional[list[type[Exception]]] = None,
        retry_limit: Optional[int] = None,
        retry_min_backoff: Optional[float] = None,
        retry_max_backoff: Optional[float] = None,
    ) -> Callable[..., Any]:
        def decorator(func: Callable[[Message], Any]) -> Handler:
            @wraps(func)
            def wrapper(message: Message) -> Any:
                return func(message)

            handler = cast(Handler, wrapper)
            handler.name = f"{func.__module__}.{func.__qualname__}"
            handler.matcher = HandlerMatcher(*matchers, operator=operator)
            handler.timeout = timeout if timeout is not None else self.config.timeout
            handler.retry_for = (
                retry_for if retry_for is not None else self.config.retry_for
            )
            handler.retry_limit = (
                retry_limit if retry_limit is not None else self.config.retry_limit
            )
            handler.retry_min_backoff = (
                retry_min_backoff
                if retry_min_backoff is not None
                else self.config.retry_min_backoff
            )
            handler.retry_max_backoff = (
                retry_max_backoff
                if retry_max_backoff is not None
                else self.config.retry_max_backoff
            )

            self.handlers.add(handler)

            return handler

        return decorator
