from itertools import cycle
from logging import INFO, Logger, basicConfig
from multiprocessing import Process
from queue import Empty, Queue
from signal import SIGINT, SIGTERM, SIG_IGN, signal
from sys import stdout
from time import sleep
from typing import Any

from .._handlers import Handler, HandlerStatus
from .._utils.logging import get_logger

from .._types import BaseModel, Message, SyncData, WorkerConfig


class Worker:
    """
    Worker is the class responsible for consuming and handling messages from the queues.
    """

    _config: WorkerConfig

    _sync_data: SyncData

    def __init__(self, config: WorkerConfig, sync_data: SyncData) -> None:
        self._config = config

        self._sync_data = sync_data

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name='{self._config.name}')"

    @property
    def _logger(self) -> Logger:
        return get_logger(name=f"Worker.{self._config.name}")

    def run(self) -> None:
        self._logger.info(
            f"{self} is starting...",
            extra={**self._config.model_dump(logging=True)},
        )

        for index, message_buffer in cycle(enumerate(self._sync_data.message_buffers)):
            if self._sync_data.shutdown.is_set():
                break

            try:
                message = message_buffer.get_nowait()
            except Empty:
                sleep(0.1)
                continue

            self._handle_message(
                message=message,
                ack_buffer=self._sync_data.ack_buffers[index],
            )

        self._logger.info(
            f"{self} stopped",
            extra={**self._config.model_dump(logging=True)},
        )

    def _handle_message(self, message: Message, ack_buffer: Queue[Any]) -> None:
        log_extras = {
            "worker": self._config.model_dump(logging=True),
            "message_id": message.id,
        }

        handlers = [
            Handler(handler=handler, message=message)
            for matcher, handler in self._sync_data.handlers
            if matcher(message)
        ]
        for handler in handlers:
            handler.run()

        handled = True
        while handlers:
            for handler in list(handlers):
                match handler.status:
                    case HandlerStatus.RUNNING:
                        if handler.duration > self._config.timeout:
                            handled = False
                            handlers.remove(handler)

                            self._logger.error(
                                f"{handler} timed out",
                                extra={**log_extras, "handler": str(handler)},
                            )
                    case HandlerStatus.FINISHED:
                        handlers.remove(handler)

                        self._logger.info(
                            f"{handler} finished",
                            extra={**log_extras, "handler": str(handler)},
                        )
                    case HandlerStatus.FAILED:
                        handled = False
                        handlers.remove(handler)

                        exception = handler.get_result(raises_on_exception=False)
                        self._logger.error(
                            f"{handler} failed with: {exception}",
                            extra={
                                **log_extras,
                                "handler": str(handler),
                                "exception": str(exception),
                            },
                        )

            sleep(0.1)

        if handled:
            ack_buffer.put(message, block=True)
        else:
            self._logger.warning(
                f"Message {message.id} handling failed",
                extra=log_extras,
            )


class RunningWorker(BaseModel):
    """
    Data class to represent the state of a live worker.

    Attributes:
        process (Process): The process running the worker.
        config (WorkerConfig): Configuration for the worker.
    """

    process: Process
    config: WorkerConfig


def run_worker(config: WorkerConfig, sync_data: SyncData) -> None:
    # Ignore signals to prevent the worker from being killed by signals
    # the parent process will handle these signals
    signal(SIGINT, SIG_IGN)
    signal(SIGTERM, SIG_IGN)

    basicConfig(level=INFO, stream=stdout)  # TODO: Improve

    Worker(config=config, sync_data=sync_data).run()
