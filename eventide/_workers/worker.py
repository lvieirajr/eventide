from datetime import datetime, timedelta, timezone
from itertools import cycle
from logging import INFO, Logger, basicConfig
from multiprocessing import Process
from queue import Empty
from signal import SIGINT, SIGTERM, SIG_IGN, signal
from sys import stdout
from time import sleep

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

        for message_queue in cycle(self._sync_data.message_queues):
            if self._sync_data.shutdown.is_set():
                break

            try:
                message = message_queue.get_nowait()
            except Empty:
                sleep(0.1)
                continue

            self._handle_message(message=message)

        self._logger.info(
            f"{self} stopped",
            extra={**self._config.model_dump(logging=True)},
        )

    def _handle_message(self, message: Message) -> None:
        log_extras = {
            "worker": self._config.model_dump(logging=True),
            "message_id": message.id,
            "retry_count": message.eventide_metadata.retry_count,
        }

        retry = self._config.retry_config
        retry_exception_types = set()
        if retry.retry is True:
            retry_exception_types = {BaseException}
        elif isinstance(retry.retry, list):
            retry_exception_types = set(retry.retry)

        failed_handlers = message.eventide_metadata.failed_handlers
        matched_handlers = {
            f"{handler.__module__}.{handler.__name__}": handler
            for matcher, handler in self._sync_data.handlers
            if matcher(message)
        }

        handlers = {
            handler_name: Handler(handler=handler, message=message)
            for handler_name, handler in matched_handlers.items()
            if not failed_handlers or handler_name in failed_handlers
        }
        for handler in handlers.values():
            handler.run()

        handled, handlers_to_retry = True, set()
        while handlers:
            for handler_name, handler in list(handlers.items()):
                match handler.status:
                    case HandlerStatus.FINISHED:
                        handlers.pop(handler_name)

                        self._logger.info(
                            f"{handler} finished",
                            extra={**log_extras, "handler": str(handler)},
                        )
                    case HandlerStatus.FAILED:
                        handled = False
                        handlers.pop(handler_name)

                        exception = handler.get_result(raises_on_exception=False)
                        if any(
                            isinstance(exception, exception_type)
                            for exception_type in retry_exception_types
                        ):
                            handlers_to_retry.add(handler_name)

                        self._logger.error(
                            f"{handler} failed with: {exception}",
                            extra={
                                **log_extras,
                                "handler": str(handler),
                                "exception": str(exception),
                            },
                        )
                    case HandlerStatus.RUNNING:
                        if handler.duration > self._config.timeout:
                            handled = False
                            handlers.pop(handler_name)

                            if retry.retry_on_timeout:
                                handlers_to_retry.add(handler_name)

                            self._logger.error(
                                f"{handler} timed out",
                                extra={**log_extras, "handler": str(handler)},
                            )

            sleep(0.1)

        if handled:
            message.ack()
        elif handlers_to_retry:
            message.eventide_metadata.retry_count += 1
            message.eventide_metadata.failed_handlers = handlers_to_retry

            retry_count = message.eventide_metadata.retry_count
            if retry_count <= retry.retry_limit:
                message.eventide_metadata.next_retry = datetime.now(
                    tz=timezone.utc
                ) + timedelta(
                    seconds=min(
                        (
                            retry.retry_min_backoff
                            * (retry.retry_backoff_multiplier**retry_count)
                        ),
                        retry.retry_max_backoff,
                    )
                )
                message.retry()

                self._logger.info(
                    f"Message {message.id} handling failed. Retrying at "
                    f"{message.eventide_metadata.next_retry}",
                    extra={
                        **log_extras,
                        "next_retry": message.eventide_metadata.next_retry,
                    },
                )
            else:
                self._logger.info(
                    f"Message {message.id} handling failed. Out of retries",
                    extra=log_extras,
                )

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
