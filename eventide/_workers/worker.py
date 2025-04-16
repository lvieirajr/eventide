from logging import Logger
from queue import Empty
from time import time

from .._queues import Queue
from .._utils.logging import get_logger
from .._types import HeartBeat, InterProcessCommunication, Message


class Worker:
    _worker_id: int
    _ipc: InterProcessCommunication

    def __init__(
        self,
        worker_id: int,
        ipc: InterProcessCommunication,
        queue: Queue,
    ) -> None:
        self._worker_id = worker_id
        self._ipc = ipc
        self.queue = queue

    @property
    def _logger(self) -> Logger:
        return get_logger(name=f"eventide.worker.{self._worker_id}")

    def run(self) -> None:
        while not self._ipc.shutdown.is_set():
            self._ipc.heartbeats.put(
                HeartBeat(
                    worker_id=self._worker_id,
                    timestamp=time(),
                    message=None,
                )
            )

            try:
                message = self.queue.get()
            except Empty:
                continue

            self._ipc.heartbeats.put(
                HeartBeat(
                    worker_id=self._worker_id,
                    timestamp=time(),
                    message=message,
                )
            )

            self._handle_message(message=message)

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

            if next_attempt <= handler.retry_limit and any(
                isinstance(exception, exception_type)
                for exception_type in handler.retry_for
            ):
                backoff = min(
                    handler.retry_max_backoff,
                    handler.retry_min_backoff * (next_attempt**2.0),
                )

                message.eventide_metadata.attempt = next_attempt
                message.eventide_metadata.retry_at = time() + backoff

                self._ipc.retries[message.id] = message

                self._logger.warning(
                    f"Retrying failed message {message.id} in {backoff:.2f}s",
                    extra={**log_extra, "backoff": backoff, "reason": type(exception)},
                )
            else:
                self._ipc.dlq.put(message)
                self._logger.warning(
                    f"Message {message.id} sent to the DLQ after exhausting all "
                    "available attempts"
                )

            return

        self._ipc.acks.put(message)
