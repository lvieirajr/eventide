from multiprocessing import get_context
from multiprocessing.context import ForkContext, ForkProcess
from multiprocessing.queues import Queue as MultiprocessingQueue
from multiprocessing.synchronize import Event as MultiprocessingEvent
from queue import Empty
from signal import SIG_IGN, SIGINT, SIGTERM, signal
from sys import exit as sys_exit
from time import time
from types import FrameType
from typing import Optional

from .._handlers import discover_handlers, handler_registry
from .._queues import Message, Queue
from .._retry import handle_failure
from .._utils import BaseModel, eventide_logger
from .._workers import HeartBeat, Worker
from .config import EventideConfig


class WorkerState(BaseModel):
    worker_id: int
    process: ForkProcess
    heartbeat: float
    message: Optional[Message] = None


class Eventide:
    _config: EventideConfig

    _context: ForkContext
    _queue: Queue[Message]
    _shutdown_event: MultiprocessingEvent
    _heartbeats: MultiprocessingQueue[HeartBeat]
    _workers: dict[int, WorkerState]

    def __init__(self, config: EventideConfig) -> None:
        self._config = config

    def run(self) -> None:
        eventide_logger.info(
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

        poll_interval, empty_polls = self._config.min_poll_interval, 0
        while not self._shutdown_event.is_set():
            self._queue.enqueue_retries()
            self._queue.enqueue_messages()

            if self._queue.empty:
                poll_interval = min(
                    self._config.max_poll_interval,
                    self._config.min_poll_interval * (2**empty_polls),
                )
                empty_polls += 1
            else:
                poll_interval, empty_polls = self._config.min_poll_interval, 0

            poll_start = time()
            while (
                time() - poll_start < poll_interval
                and not self._shutdown_event.is_set()
            ):
                self._monitor_workers()

        eventide_logger.info(
            "Stopping Eventide...",
            extra={"config": self._config.model_dump()},
        )

        self._shutdown(force=False)

    def _discover_handlers(self) -> None:
        discover_handlers(self._config.handler_paths)

        for handler in handler_registry:
            if handler.timeout is None:
                handler.timeout = self._config.timeout
            else:
                handler.timeout = max(handler.timeout, 0.001)

            if handler.retry_for is None:
                handler.retry_for = list(self._config.retry_for)
            else:
                handler.retry_for = list(handler.retry_for)

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
                eventide_logger.info(
                    "Shutting down gracefully...",
                    extra={
                        "config": self._config.model_dump(),
                        "signal": signum,
                        "frame": frame,
                    },
                )
                self._shutdown_event.set()
            else:
                eventide_logger.info(
                    "Forcing immediate shutdown...",
                    extra={
                        "config": self._config.model_dump(),
                        "signal": signum,
                        "frame": frame,
                    },
                )
                self._shutdown(force=True)
                sys_exit(1)

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
            current_worker.process.terminate()
            current_worker.process.kill()
            current_worker.process.join()

    def _monitor_workers(self) -> None:
        while True:
            try:
                heartbeat_obj = self._heartbeats.get_nowait()
            except Empty:
                break

            self._workers[heartbeat_obj.worker_id] = WorkerState(
                worker_id=heartbeat_obj.worker_id,
                process=self._workers[heartbeat_obj.worker_id].process,
                heartbeat=heartbeat_obj.timestamp,
                message=heartbeat_obj.message,
            )

        for worker_id, worker_state in list(self._workers.items()):
            heartbeat = worker_state.heartbeat
            message = worker_state.message
            handler = message.eventide_metadata.handler if message else None

            if message and handler and (time() - heartbeat) > handler.timeout:
                self._kill_worker(worker_id=worker_id)

                if not self._shutdown_event.is_set():
                    self._spawn_worker(worker_id=worker_id)

                    handle_failure(
                        message=message,
                        queue=self._queue,
                        exception=TimeoutError("Handler timed out"),
                    )
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
