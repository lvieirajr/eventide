from functools import cached_property
from logging import Logger, getLogger
from multiprocessing.context import ForkContext, ForkProcess
from multiprocessing import Event as MultiprocessingEvent
from multiprocessing import Queue as MultiprocessingQueue
from multiprocessing import get_context
from queue import Empty
from signal import SIGINT, SIGTERM, SIG_IGN, signal
from sys import exit
from time import sleep, time
from types import FrameType
from typing import Optional

from .config import EventideConfig
from .._handlers import discover_handlers, handler_registry
from .._types import BaseModel
from .._queues import Message, Queue
from .._workers import Worker


class WorkerState(BaseModel):
    worker_id: int
    process: ForkProcess
    heartbeat: float
    message: Optional[Message] = None


class Eventide:
    _config: EventideConfig

    _context: ForkContext
    _queue: Queue
    _shutdown_event: MultiprocessingEvent
    _heartbeats: MultiprocessingQueue
    _workers: dict[int, WorkerState]

    def __init__(self, config: EventideConfig) -> None:
        self._config = config

    @cached_property
    def _logger(self) -> Logger:
        return getLogger(name="eventide")

    def run(self) -> None:
        self._logger.info(
            "Starting Eventide...",
            extra={"config": self._config.model_dump()},
        )

        self._discover_handlers()
        self._setup_signal_handlers()

        self._context = get_context("fork")
        self._shutdown_event = self._context.Event()

        self._queue = Queue.factory(config=self._config.queue, context=self._context)

        self._heartbeats = self._context.Queue()

        self._workers = {}
        for worker_id in range(1, self._config.concurrency + 1):
            self._spawn_worker(worker_id=worker_id)

        while not self._shutdown_event.is_set():
            self._monitor_workers()

            if not self._queue.full:
                self._queue.enqueue_retries()

            if not self._queue.full:
                self._queue.enqueue_messages()

            sleep(0.1)

        self._logger.info(
            "Stopping Eventide...",
            extra={"config": self._config.model_dump()},
        )

        self._shutdown(force=False)

    def _discover_handlers(self) -> None:
        discover_handlers(self._config.handler_paths)

        for _, handler in handler_registry:
            if hasattr(handler, "retry_for"):
                if handler.timeout is None:
                    handler.timeout = self._config.timeout
                else:
                    handler.timeout = max(handler.timeout, 1e-9)

                if handler.retry_for is None:
                    handler.retry_for = set(self._config.retry_for)
                else:
                    handler.retry_for = set(handler.retry_for)

                if handler.retry_limit is None:
                    handler.retry_limit = self._config.retry_limit
                else:
                    handler.retry_limit = max(handler.retry_limit, 0)

                if handler.retry_min_backoff is None:
                    handler.retry_min_backoff = self._config.retry_min_backoff
                else:
                    handler.retry_min_backoff = max(handler.retry_min_backoff, 0)

                if handler.retry_max_backoff is None:
                    handler.retry_max_backoff = self._config.retry_max_backoff
                else:
                    handler.retry_max_backoff = max(handler.retry_max_backoff, 0)

    def _setup_signal_handlers(self) -> None:
        def handle_signal(signum: int, frame: Optional[FrameType]) -> None:
            if not self._shutdown_event.is_set():
                self._logger.info(
                    "Shutting down gracefully...",
                    extra={
                        "config": self._config.model_dump(),
                        "signal": signum,
                        "frame": frame,
                    },
                )
                self._shutdown_event.set()
            else:
                self._logger.info(
                    "Forcing immediate shutdown...",
                    extra={
                        "config": self._config.model_dump(),
                        "signal": signum,
                        "frame": frame,
                    },
                )
                self._shutdown(force=True)
                exit(1)

        signal(SIGINT, handle_signal)
        signal(SIGTERM, handle_signal)

    def _spawn_worker(self, worker_id: int) -> None:
        def _worker_process() -> None:
            signal(SIGINT, SIG_IGN)
            signal(SIGTERM, SIG_IGN)

            Worker(
                worker_id=worker_id,
                queue=self._queue,
                shutdown_event=self._shutdown_event,
                heartbeats=self._heartbeats,
            ).run()

        self._workers[worker_id] = WorkerState(
            worker_id=worker_id,
            process=self._context.Process(target=_worker_process, daemon=True),
            heartbeat=time(),
            message=None,
        )
        self._workers[worker_id].process.start()

    def _kill_worker(self, worker_id: int) -> None:
        current_worker = self._workers.pop(worker_id, None)

        if current_worker and current_worker.process.is_alive():
            current_worker.process.kill()
            current_worker.process.join()

    def _monitor_workers(self) -> None:
        while True:
            try:
                heartbeat = self._heartbeats.get_nowait()
            except Empty:
                break

            self._workers[heartbeat.worker_id] = WorkerState(
                worker_id=heartbeat.worker_id,
                process=self._workers[heartbeat.worker_id].process,
                heartbeat=heartbeat.timestamp,
                message=heartbeat.message,
            )

        for worker_id, worker_state in list(self._workers.items()):
            heartbeat = worker_state.heartbeat
            message = worker_state.message
            handler = message.eventide_metadata.handler if message else None

            if message and handler and (time() - heartbeat) > handler.timeout:
                next_attempt = message.eventide_metadata.attempt + 1

                self._kill_worker(worker_id=worker_id)
                if not self._shutdown_event.is_set():
                    self._spawn_worker(worker_id=worker_id)

                    if next_attempt <= (handler.retry_limit + 1) and any(
                        issubclass(exception_type, TimeoutError)
                        for exception_type in handler.retry_for
                    ):
                        backoff = min(
                            handler.retry_max_backoff,
                            handler.retry_min_backoff * 2 ** (next_attempt - 2),
                        )

                        message.eventide_metadata.attempt = next_attempt
                        message.eventide_metadata.retry_at = time() + backoff

                        self._queue.retry_message(message)
                    else:
                        self._queue.dlq_message(message)
            elif not self._workers[worker_id].process.is_alive():
                if self._shutdown_event.is_set():
                    self._kill_worker(worker_id=worker_id)
                else:
                    self._spawn_worker(worker_id=worker_id)

    def _shutdown(self, force: bool = False) -> None:
        if not force:
            while self._workers:
                self._monitor_workers()

        for worker_id in list(self._workers.keys()):
            self._kill_worker(worker_id=worker_id)

        self._heartbeats.close()
        self._heartbeats.cancel_join_thread()

        self._queue.shutdown()
