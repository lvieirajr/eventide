from multiprocessing import get_context
from multiprocessing.context import ForkContext, ForkProcess
from multiprocessing.queues import Queue as MultiprocessingQueue
from queue import Empty
from signal import SIG_IGN, SIGINT, SIGTERM, signal
from sys import exit as sys_exit
from time import sleep, time
from types import FrameType
from typing import Callable, Optional

from .._exceptions import WorkerCrashedError
from .._handlers import Handler
from .._queues import Message
from .._utils.logging import eventide_logger
from .._utils.pydantic import PydanticModel
from .._utils.retry import handle_failure
from .._workers import HeartBeat, Worker
from .config import EventideConfig
from .handler import HandlerManager
from .queue import QueueManager


class WorkerState(PydanticModel):
    worker_id: int
    process: ForkProcess
    heartbeat: float
    message: Optional[Message] = None


class Eventide:
    _config: EventideConfig

    context: ForkContext
    heartbeats: MultiprocessingQueue[HeartBeat]

    handler_manager: HandlerManager
    queue_manager: QueueManager

    _workers: dict[int, WorkerState]

    def __init__(self, config: EventideConfig) -> None:
        self._config = config

        self.context = get_context("fork")
        self.shutdown_event = self.context.Event()
        self.shutdown_event.set()

        self.handler_manager = HandlerManager(config=self._config)
        self.queue_manager = QueueManager(
            config=self._config,
            context=self.context,
            handler_manager=self.handler_manager,
        )

    @property
    def handler(self) -> Callable[..., Callable[..., Handler]]:
        return self.handler_manager.handler

    def run(self) -> None:
        eventide_logger.info("Starting Eventide...")

        self.shutdown_event.clear()
        self.heartbeats = self.context.Queue()

        self._setup_signal_handlers()

        self.queue_manager.start()

        self._workers = {}
        for worker_id in range(1, self._config.concurrency + 1):
            self._spawn_worker(worker_id)

        while not self.shutdown_event.is_set():
            self.queue_manager.enqueue_retries()
            self.queue_manager.enqueue_messages()

            pull_start = time()
            while (
                time() - pull_start < self.queue_manager.pull_interval
                and not self.shutdown_event.is_set()
            ):
                self._monitor_workers()

        eventide_logger.info("Stopping Eventide...")

        self._shutdown(force=False)

    def _setup_signal_handlers(self) -> None:
        def handle_signal(_signum: int, _frame: Optional[FrameType]) -> None:
            if not self.shutdown_event.is_set():
                eventide_logger.info("Shutting down gracefully...")
                self.shutdown_event.set()
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
            Worker(
                worker_id,
                self.queue_manager.queue,
                self.shutdown_event,
                self.heartbeats,
            ).run()

        self._workers[worker_id] = WorkerState(
            worker_id=worker_id,
            process=self.context.Process(target=_worker_process, daemon=True),
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
                heartbeat_obj = self.heartbeats.get_nowait()
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

                if not self.shutdown_event.is_set():
                    self._spawn_worker(worker_id)

                if worker_state.message:
                    handle_failure(
                        worker_state.message,
                        self.queue_manager.queue,
                        WorkerCrashedError(
                            f"Worker {worker_id} crashed while handling message "
                            f"{worker_state.message.id}",
                        ),
                    )

        sleep(0.1)

    def _shutdown(self, force: bool = False) -> None:
        self.shutdown_event.set()

        if not force:
            while self._workers:
                self._monitor_workers()

        for worker_id in list(self._workers.keys()):
            self._kill_worker(worker_id)

        self.heartbeats.close()
        self.heartbeats.cancel_join_thread()

        self.queue_manager.shutdown()
