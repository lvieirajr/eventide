from typing import Callable

from .._queues import Message


class HookManager:
    _on_start_hooks: dict[str, Callable[[], None]]
    _on_shutdown_hooks: dict[str, Callable[[], None]]
    _on_message_received_hooks: dict[str, Callable[[Message], None]]
    _on_message_success_hooks: dict[str, Callable[[Message], None]]
    _on_message_failure_hooks: dict[str, Callable[[Message, Exception], None]]

    def __init__(self) -> None:
        self._on_start_hooks = {}
        self._on_shutdown_hooks = {}
        self._on_message_received_hooks = {}
        self._on_message_success_hooks = {}
        self._on_message_failure_hooks = {}

    def register_start_hook(self, hook: Callable[[], None]) -> None:
        self._on_start_hooks[f"{hook.__module__}.{hook.__qualname__}"] = hook

    def register_shutdown_hook(self, hook: Callable[[], None]) -> None:
        self._on_shutdown_hooks[f"{hook.__module__}.{hook.__qualname__}"] = hook

    def register_message_received_hook(self, hook: Callable[[Message], None]) -> None:
        self._on_message_received_hooks[f"{hook.__module__}.{hook.__qualname__}"] = hook

    def register_message_success_hook(self, hook: Callable[[Message], None]) -> None:
        self._on_message_success_hooks[f"{hook.__module__}.{hook.__qualname__}"] = hook

    def register_message_failure_hook(
        self,
        hook: Callable[[Message, Exception], None],
    ) -> None:
        self._on_message_failure_hooks[f"{hook.__module__}.{hook.__qualname__}"] = hook

    def on_start(self) -> None:
        for hook in self._on_start_hooks.values():
            hook()

    def on_shutdown(self) -> None:
        for hook in self._on_shutdown_hooks.values():
            hook()

    def on_message_received(self, message: Message) -> None:
        for hook in self._on_message_received_hooks.values():
            hook(message)

    def on_message_success(self, message: Message) -> None:
        for hook in self._on_message_success_hooks.values():
            hook(message)

    def on_message_failure(self, message: Message, exception: Exception) -> None:
        for hook in self._on_message_failure_hooks.values():
            hook(message, exception)
