from multiprocessing.queues import Queue as MultiprocessingQueue
from multiprocessing.synchronize import Event as MultiprocessingEvent
from queue import Empty, ShutDown
from time import sleep, time
from typing import Optional

from .._queues import Message, Queue
from .._retry import handle_failure
from .._utils import BaseModel, worker_logger


class HeartBeat(BaseModel):
    worker_id: int
    timestamp: float
    message: Optional[Message] = None


class Worker:
    _worker_id: int
    _queue: Queue[Message]
    _shutdown: MultiprocessingEvent
    _heartbeats: MultiprocessingQueue[HeartBeat]

    def __init__(
        self,
        worker_id: int,
        queue: Queue[Message],
        shutdown_event: MultiprocessingEvent,
        heartbeats: MultiprocessingQueue[HeartBeat],
    ) -> None:
        self._worker_id = worker_id
        self._queue = queue
        self._shutdown_event = shutdown_event
        self._heartbeats = heartbeats

    def run(self) -> None:
        while not self._shutdown_event.is_set():
            self._heartbeat(message=None)

            message = self._get_message()
            if message:
                self._heartbeat(message=message)
                self._handle_message(message=message)
                self._heartbeat(message=None)
            else:
                sleep(0.1)

    def _handle_message(self, message: Message) -> None:
        handler, start = message.eventide_metadata.handler, time()

        worker_logger.info(
            f"Message {message.id} received",
            extra={
                "message_id": message.id,
                "handler": handler.name,
                "attempt": message.eventide_metadata.attempt,
            },
        )
        try:
            message.eventide_metadata.handler(message)
        except Exception as exception:
            handle_failure(message=message, queue=self._queue, exception=exception)
        else:
            end = time()

            self._queue.ack_message(message=message)

            worker_logger.info(
                f"Message {message.id} handling succeeded in {end - start}s",
                extra={
                    "message_id": message.id,
                    "handler": handler.name,
                    "attempt": message.eventide_metadata.attempt,
                    "duration": end - start,
                },
            )

    def _get_message(self) -> Optional[Message]:
        try:
            return self._queue.get_message()
        except (Empty, ShutDown):
            return None

    def _heartbeat(self, message: Optional[Message] = None) -> None:
        try:
            self._heartbeats.put_nowait(
                HeartBeat(worker_id=self._worker_id, timestamp=time(), message=message),
            )
        except (Empty, ShutDown):
            pass
