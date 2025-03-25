from functools import wraps
from typing import Any, Literal

from .matcher import HandlerMatcher
from .._types import AnyCallableType, HandlerFuncType


def eventide_handler(
    *matchers: str,
    operator: Literal["all", "any", "and", "or"] = "all",
) -> AnyCallableType:
    """
    Decorator to register a function as a message handler.

    Parameters:
        matchers (str): One or more JMESPath expressions that will be used to match the
            message.
        operator (Literal["all", "any", "and", "or"]): The operator to use
            when evaluating the matchers. Defaults to "all".

    Returns:
        AnyCallableType: The decorated function.
    """

    def decorator(func: HandlerFuncType) -> AnyCallableType:
        from .registry import handler_registry

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        handler_registry.add((HandlerMatcher(*matchers, operator=operator), wrapper))

        return wrapper

    return decorator
