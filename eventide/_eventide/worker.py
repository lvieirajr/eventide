from multiprocessing.context import ForkContext, ForkProcess
from multiprocessing.queues import Queue as MultiprocessingQueue
from multiprocessing.synchronize import Event as MultiprocessingEvent
from queue import Empty
from signal import SIG_IGN, SIGINT, SIGTERM, signal
from time import sleep, time
from typing import Optional

from .._exceptions import WorkerCrashedError
from .._queues import Message
from .._utils.pydantic import PydanticModel
from .._utils.retry import handle_failure
from .._workers import HeartBeat, Worker
from .config import EventideConfig
from .queue import QueueManager


class WorkerState(PydanticModel):
    worker_id: int
    process: ForkProcess
    heartbeat: float
    message: Optional[Message] = None


class WorkerManager:
    config: EventideConfig
    context: ForkContext

    queue_manager: QueueManager

    _shutdown_event: MultiprocessingEvent
    _heartbeats: MultiprocessingQueue[HeartBeat]
    _workers: dict[int, WorkerState]

    def __init__(
        self,
        config: EventideConfig,
        context: ForkContext,
        queue_manager: QueueManager,
    ) -> None:
        self.config = config
        self.context = context

        self.queue_manager = queue_manager

    @property
    def is_shutdown(self) -> bool:
        return self._shutdown_event.is_set()

    def start(self) -> None:
        self._shutdown_event = self.context.Event()
        self._heartbeats = self.context.Queue()

        self._workers = {}
        for worker_id in range(1, self.config.concurrency + 1):
            self.spawn_worker(worker_id=worker_id)

    def send_shutdown_event(self) -> None:
        self._shutdown_event.set()

    def shutdown(self, force: bool = False) -> None:
        self.send_shutdown_event()

        if not force:
            while self._workers:
                self.monitor_workers()

        for worker_id in list(self._workers.keys()):
            self.kill_worker(worker_id=worker_id)

        self._heartbeats.close()
        self._heartbeats.cancel_join_thread()

    def monitor_workers(self) -> None:
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
            if not worker_state.process.is_alive():
                self.kill_worker(worker_id=worker_id)

                if not self.is_shutdown:
                    self.spawn_worker(worker_id=worker_id)

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

    def spawn_worker(self, worker_id: int) -> None:
        def worker_process() -> None:
            signal(SIGINT, SIG_IGN)
            signal(SIGTERM, SIG_IGN)

            Worker(
                worker_id=worker_id,
                queue=self.queue_manager.queue,
                shutdown_event=self._shutdown_event,
                heartbeats=self._heartbeats,
            ).run()

        self._workers[worker_id] = WorkerState(
            worker_id=worker_id,
            process=self.context.Process(target=worker_process, daemon=True),
            heartbeat=time(),
            message=None,
        )
        self._workers[worker_id].process.start()

    def kill_worker(self, worker_id: int) -> None:
        worker = self._workers.pop(worker_id, None)

        if worker:
            if worker.process.is_alive():
                worker.process.terminate()

            if worker.process.is_alive():
                worker.process.kill()

            worker.process.join()
