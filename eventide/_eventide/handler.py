from collections.abc import Iterable
from functools import wraps
from importlib import import_module
from pathlib import Path
from pkgutil import walk_packages
from sys import path
from typing import Any, Callable, Optional, Union, cast

from .._handlers import Handler, HandlerMatcher, MatcherCallable
from .._queues import Message
from .._utils.logging import eventide_logger
from .config import EventideConfig


class HandlerManager:
    config: EventideConfig

    def __init__(self, config: EventideConfig) -> None:
        self.config = config

        self._handlers: set[Handler] = set()
        self._discovered: bool = False

    @property
    def handlers(self) -> set[Handler]:
        if not self._discovered:
            self.discover_handlers()

        return self._handlers

    def handler(
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

    def discover_handlers(self) -> None:
        for raw_path in set(self.config.handler_paths) or {"."}:
            resolved_path = Path(raw_path).resolve()

            if not resolved_path.exists():
                eventide_logger.debug(f"Path '{resolved_path}' does not exist")
                continue

            base = str(
                resolved_path.parent if resolved_path.is_file() else resolved_path
            )
            if base not in path:
                path.insert(0, base)

            if resolved_path.is_file() and resolved_path.suffix == ".py":
                name = resolved_path.stem

                try:
                    import_module(name)
                except (ImportError, TypeError):
                    eventide_logger.debug(f"Failed to discover handlers from '{name}'")

                continue

            if resolved_path.is_dir():
                init_file = resolved_path / "__init__.py"

                if not init_file.exists():
                    eventide_logger.debug(
                        f"Directory '{resolved_path}' is not a Python package",
                    )
                    continue

                name = resolved_path.name
                try:
                    module = import_module(name)
                except (ImportError, TypeError):
                    eventide_logger.debug(f"Failed to discover handlers from '{name}'")
                    continue

                for _, module_name, is_package in walk_packages(
                    module.__path__,
                    prefix=module.__name__ + ".",
                ):
                    if is_package:
                        continue

                    try:
                        import_module(module_name)
                    except (ImportError, TypeError):
                        eventide_logger.debug(
                            f"Failed to discover handlers from '{module_name}'",
                        )

        self._discovered = True
