from datetime import datetime, timezone
from enum import StrEnum, auto

from threading import Thread
from typing import Any, Optional

from .._types import HandlerFuncType, Message


class HandlerStatus(StrEnum):
    PENDING = auto()
    RUNNING = auto()
    FINISHED = auto()
    FAILED = auto()


class Handler:
    def __init__(self, handler: HandlerFuncType, message: Message) -> None:
        self.handler = handler
        self.message = message

        self.start_dt: Optional[datetime] = None
        self.end_dt: Optional[datetime] = None

        self._thread: Optional[Thread] = None

        self._result: Any = None

    def __repr__(self) -> str:
        return (
            f"<"
            f"Eventide.{type(self).__name__} "
            f"{self.handler.__name__} "
            f"{self.message.id}"
            f">"
        )

    def __str__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"handler='{self.handler.__name__}', "
            f"message='{self.message.id}'"
            f")"
        )

    @property
    def status(self) -> HandlerStatus:
        if not self.start_dt:
            return HandlerStatus.PENDING
        elif bool(self._thread and self._thread.is_alive()):
            return HandlerStatus.RUNNING
        elif not isinstance(self._result, Exception):
            return HandlerStatus.FINISHED

        return HandlerStatus.FAILED

    @property
    def duration(self) -> float:
        if not self.start_dt:
            return 0.0

        end = self.end_dt if self.end_dt else datetime.now(tz=timezone.utc)

        return (end - self.start_dt).total_seconds()

    def get_result(self, raises_on_exception: bool = True) -> Any:
        if self.status != HandlerStatus.FINISHED:
            raise RuntimeError("Handler is not finished")

        if raises_on_exception and isinstance(self._result, Exception):
            raise self._result

        return self._result

    def run(self) -> None:
        self._thread = Thread(
            target=self._handler_loop,
            name=f"Eventide.Handler.{self.handler.__name__}.{self.message.id}",
            daemon=True,
        )
        self._thread.start()

    def _handler_loop(self) -> None:
        self.start_dt = datetime.now(tz=timezone.utc)

        try:
            self._result = self.handler(self.message)
        except Exception as exception:
            self._result = exception
        finally:
            self.end_dt = datetime.now(tz=timezone.utc)
