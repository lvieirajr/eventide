from typing import Callable

from .._queues import Message


class HookManager:
    _on_start_hooks: list[Callable[[], None]]
    _on_shutdown_hooks: list[Callable[[], None]]
    _on_message_received_hooks: list[Callable[[Message], None]]
    _on_message_success_hooks: list[Callable[[Message], None]]
    _on_message_failure_hooks: list[Callable[[Message, Exception], None]]

    def __init__(self) -> None:
        self._on_start_hooks = []
        self._on_shutdown_hooks = []
        self._on_message_received_hooks = []
        self._on_message_success_hooks = []
        self._on_message_failure_hooks = []

    def register_start_hook(self, hook: Callable[[], None]) -> None:
        self._on_start_hooks.append(hook)

    def register_shutdown_hook(self, hook: Callable[[], None]) -> None:
        self._on_shutdown_hooks.append(hook)

    def register_message_received_hook(self, hook: Callable[[Message], None]) -> None:
        self._on_message_received_hooks.append(hook)

    def register_message_success_hook(self, hook: Callable[[Message], None]) -> None:
        self._on_message_success_hooks.append(hook)

    def register_message_failure_hook(
        self,
        hook: Callable[[Message, Exception], None],
    ) -> None:
        self._on_message_failure_hooks.append(hook)

    def on_start(self) -> None:
        for hook in self._on_start_hooks:
            hook()

    def on_shutdown(self) -> None:
        for hook in self._on_shutdown_hooks:
            hook()

    def on_message_received(self, message: Message) -> None:
        for hook in self._on_message_received_hooks:
            hook(message)

    def on_message_success(self, message: Message) -> None:
        for hook in self._on_message_success_hooks:
            hook(message)

    def on_message_failure(self, message: Message, exception: Exception) -> None:
        for hook in self._on_message_failure_hooks:
            hook(message, exception)
