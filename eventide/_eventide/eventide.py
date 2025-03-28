from logging import Logger
from os import _exit
from signal import SIGINT, SIGTERM, Signals, signal
from multiprocessing import Manager, Process, set_start_method
from multiprocessing.managers import SyncManager
from threading import Thread
from time import sleep
from types import FrameType
from typing import Optional

from .._types import EventideConfig, SyncData, WorkerConfig
from .._queues import Queue, RunningQueue
from .._workers import RunningWorker, run_worker
from .._utils.logging import get_logger

set_start_method("fork", force=True)


class Eventide:
    """
    Eventide orchestrates the lifecycle of the application.
    It manages the configured queues and worker instances, handling graceful startup
    and shutdown based on system signals.
    """

    _instance: "Eventide"

    _config: EventideConfig

    _sync_manager: SyncManager

    _queues: list[RunningQueue]
    _workers: list[RunningWorker]

    def __new__(cls, config: EventideConfig) -> "Eventide":
        if not config.queues:
            raise ValueError("Eventide requires at least one queue")

        if not config.workers:
            raise ValueError("Eventide requires at least one worker")

        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)

            cls._sync_manager = Manager()
            cls._sync_data = SyncData(
                shutdown=cls._sync_manager.Event(),
                message_queues=[],
                ack_queues=[],
                retry_dicts=[],
                handlers=set(),
            )

            cls._queues = []
            cls._workers = []

        cls._config = config

        return cls._instance

    @property
    def _logger(self) -> Logger:
        return get_logger()

    def run(self) -> None:
        self._discover_handlers()
        self._setup_signal_handlers()

        self._sync_data.shutdown.clear()
        self._logger.info("Starting Eventide...")

        self._spawn_queues()
        self._spawn_workers()

        while True:
            queues_alive = self._evaluate_queues()
            workers_alive = self._evaluate_workers()

            if self._sync_data.shutdown.is_set() and not (
                queues_alive or workers_alive
            ):
                break

            sleep(0.1)

    def _discover_handlers(self) -> None:
        from .._handlers import discover_handlers, handler_registry

        discover_handlers(self._config.handler_discovery_paths)

        self._sync_data.handlers = handler_registry

    def _setup_signal_handlers(self) -> None:
        def handle_signal(signal_number: int, _f: Optional[FrameType]) -> None:
            signal_name = Signals(signal_number).name

            if not self._sync_data.shutdown.is_set():
                self._logger.info(
                    f"{signal_name} received. Initiating graceful shutdown...",
                )
                self._sync_data.shutdown.set()
            else:
                self._logger.warning(
                    f"{signal_name} received. Forcing immediate shutdown..."
                )

                _exit(1)

        signal(SIGINT, handle_signal)
        signal(SIGTERM, handle_signal)

    def _spawn_queues(self) -> None:
        self._queues = []

        for queue_config in self._config.queues:
            self._sync_data.message_queues.append(
                self._sync_manager.Queue(maxsize=queue_config.size),
            )
            self._sync_data.ack_queues.append(self._sync_manager.Queue())
            self._sync_data.retry_dicts.append(self._sync_manager.dict())

            queue = Queue.factory(config=queue_config, sync_data=self._sync_data)

            thread = Thread(target=queue.run, name=f"Eventide.{queue}", daemon=True)
            thread.start()

            self._queues.append(
                RunningQueue(queue=queue, thread=thread, config=queue_config),
            )

    def _spawn_workers(self) -> None:
        worker_configs = self._config.workers

        if isinstance(worker_configs, int):
            worker_configs = [WorkerConfig() for _ in range(worker_configs)]

        self._workers = []
        for worker_config in worker_configs:
            process = Process(
                target=run_worker,
                kwargs={"config": worker_config, "sync_data": self._sync_data},
                name=f"Eventide.Worker.{worker_config.name}",
                daemon=True,
            )
            process.start()

            self._workers.append(
                RunningWorker(process=process, config=worker_config),
            )

    def _evaluate_queues(self) -> bool:
        any_queues_running = False

        for queue in self._queues:
            if queue.thread.is_alive():
                any_queues_running = True

        return any_queues_running

    def _evaluate_workers(self) -> bool:
        any_workers_running = False

        for worker in self._workers:
            if worker.process.is_alive():
                any_workers_running = True

        return any_workers_running
