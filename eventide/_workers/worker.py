from logging import Logger
from multiprocessing import Event as MultiprocessingEvent
from multiprocessing import Queue as MultiprocessingQueue
from queue import Empty, ShutDown
from time import sleep, time
from typing import Optional

from .._queues import Queue
from .._utils.logging import get_logger
from .._types import HeartBeat, Message


class Worker:
    _worker_id: int
    _queue: Queue
    _shutdown: MultiprocessingEvent
    _heartbeats: MultiprocessingQueue

    def __init__(
        self,
        worker_id: int,
        queue: Queue,
        shutdown_event: MultiprocessingEvent,
        heartbeats: MultiprocessingQueue,
    ) -> None:
        self._worker_id = worker_id
        self._queue = queue
        self._shutdown_event = shutdown_event
        self._heartbeats = heartbeats

    @property
    def _logger(self) -> Logger:
        return get_logger(name=f"eventide.worker.{self._worker_id}")

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
        handler = message.eventide_metadata.handler

        log_extra = {
            "worker": self._worker_id,
            "message_id": message.id,
            "handler": f"{handler.__module__}.{handler.__name__}",
            "attempt": message.eventide_metadata.attempt,
        }

        try:
            handler(message)
        except Exception as exception:
            next_attempt = message.eventide_metadata.attempt + 1

            if next_attempt <= (handler.retry_limit + 1) and any(
                isinstance(exception, exception_type)
                for exception_type in handler.retry_for
            ):
                backoff = min(
                    handler.retry_max_backoff,
                    handler.retry_min_backoff * 2 ** (next_attempt - 2),
                )

                message.eventide_metadata.attempt = next_attempt
                message.eventide_metadata.retry_at = time() + backoff

                self._queue.retry_message(message=message)

                self._logger.warning(
                    f"Retrying failed message {message.id} in {backoff:.2f}s",
                    extra={**log_extra, "backoff": backoff, "reason": type(exception)},
                )
            else:
                self._queue.dlq_message(message=message)
                self._logger.warning(
                    f"Message {message.id} sent to the DLQ after exhausting all "
                    "available attempts"
                )

            return

        self._queue.ack_message(message=message)

    def _get_message(self) -> Optional[Message]:
        try:
            return self._queue.get_message()
        except (Empty, ShutDown):
            return None

    def _heartbeat(self, message: Optional[Message] = None) -> None:
        try:
            self._heartbeats.put_nowait(
                HeartBeat(worker_id=self._worker_id, timestamp=time(), message=message)
            )
        except (Empty, ShutDown):
            pass
