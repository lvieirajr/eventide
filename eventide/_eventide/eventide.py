from multiprocessing import get_context
from multiprocessing.context import ForkContext, ForkProcess
from multiprocessing.queues import Queue as MultiprocessingQueue
from multiprocessing.synchronize import Event as MultiprocessingEvent
from queue import Empty
from signal import SIG_IGN, SIGINT, SIGTERM, signal
from sys import exit as sys_exit
from time import sleep, time
from types import FrameType
from typing import Callable, Optional

from .._exceptions import WorkerCrashedError
from .._handlers import Handler
from .._queues import Message, Queue
from .._utils.logging import eventide_logger
from .._utils.pydantic import PydanticModel
from .._utils.retry import handle_failure
from .._workers import HeartBeat, Worker
from .config import EventideConfig
from .handler import HandlerManager


class WorkerState(PydanticModel):
    worker_id: int
    process: ForkProcess
    heartbeat: float
    message: Optional[Message] = None


class Eventide:
    _config: EventideConfig

    handler_manager: HandlerManager

    _context: ForkContext
    _queue: Queue[Message]
    _shutdown_event: MultiprocessingEvent
    _heartbeats: MultiprocessingQueue[HeartBeat]
    _workers: dict[int, WorkerState]

    def __init__(self, config: EventideConfig) -> None:
        self._config = config
        self.handler_manager = HandlerManager(config=self._config)

    @property
    def handler(self) -> Callable[..., Callable[..., Handler]]:
        return self.handler_manager.handler

    def run(self) -> None:
        eventide_logger.info("Starting Eventide...")

        self._setup_signal_handlers()

        self._context = get_context("fork")
        self._shutdown_event = self._context.Event()

        self._queue = Queue.factory(config=self._config.queue, context=self._context)

        self._heartbeats = self._context.Queue()

        self._workers = {}
        for worker_id in range(1, self._config.concurrency + 1):
            self._spawn_worker(worker_id)

        pull_interval, empty_pulls = self._config.min_pull_interval, 0
        while not self._shutdown_event.is_set():
            self._enqueue_retries()
            self._enqueue_messages()

            if self._queue.empty:
                pull_interval = min(
                    self._config.max_pull_interval,
                    self._config.min_pull_interval * (2**empty_pulls),
                )
                empty_pulls += 1
            else:
                pull_interval, empty_pulls = self._config.min_pull_interval, 0

            pull_start = time()
            while (
                time() - pull_start < pull_interval
                and not self._shutdown_event.is_set()
            ):
                self._monitor_workers()

        eventide_logger.info("Stopping Eventide...")

        self._shutdown(force=False)

    def _setup_signal_handlers(self) -> None:
        def handle_signal(_signum: int, _frame: Optional[FrameType]) -> None:
            if not self._shutdown_event.is_set():
                eventide_logger.info("Shutting down gracefully...")
                self._shutdown_event.set()
            else:
                eventide_logger.info("Forcing immediate shutdown...")
                self._shutdown(force=True)
                sys_exit(1)

        signal(SIGINT, handle_signal)
        signal(SIGTERM, handle_signal)

    def _spawn_worker(self, worker_id: int) -> None:
        def _worker_process() -> None:
            signal(SIGINT, SIG_IGN)
            signal(SIGTERM, SIG_IGN)
            Worker(worker_id, self._queue, self._shutdown_event, self._heartbeats).run()

        self._workers[worker_id] = WorkerState(
            worker_id=worker_id,
            process=self._context.Process(target=_worker_process, daemon=True),
            heartbeat=time(),
            message=None,
        )
        self._workers[worker_id].process.start()

    def _kill_worker(self, worker_id: int) -> None:
        current_worker = self._workers.pop(worker_id, None)

        if current_worker:
            if current_worker.process.is_alive():
                current_worker.process.terminate()

            if current_worker.process.is_alive():
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
            if not worker_state.process.is_alive():
                self._kill_worker(worker_id)

                if not self._shutdown_event.is_set():
                    self._spawn_worker(worker_id)

                if worker_state.message:
                    handle_failure(
                        worker_state.message,
                        self._queue,
                        WorkerCrashedError(
                            f"Worker {worker_id} crashed while handling message "
                            f"{worker_state.message.id}",
                        ),
                    )

        sleep(0.1)

    def _enqueue_retries(self) -> None:
        retry_messages = []

        while True:
            try:
                retry_messages.append(self._queue.get_retry_message())
            except Empty:
                break

        for message in sorted(
            retry_messages,
            key=lambda m: m.eventide_metadata.retry_at,
        ):
            if message.eventide_metadata.retry_at <= time() and not self._queue.full:
                self._queue.put_message(message)
                continue

            self._queue.put_retry_message(message)

    def _enqueue_messages(self) -> None:
        if not self._queue.should_pull:
            return

        for message in self._queue.pull_messages():
            for handler in self.handler_manager.handlers:
                if handler.matcher(message):
                    message.eventide_metadata.handler = handler

                    self._queue.put_message(message)
                    break

            if not message.eventide_metadata.handler:
                eventide_logger.error(
                    f"No handler found for message {message.id}",
                    extra={"message_id": message.id},
                )

    def _shutdown(self, force: bool = False) -> None:
        self._shutdown_event.set()

        if not force:
            while self._workers:
                self._monitor_workers()

        for worker_id in list(self._workers.keys()):
            self._kill_worker(worker_id)

        self._heartbeats.close()
        self._heartbeats.cancel_join_thread()

        self._queue.shutdown()
