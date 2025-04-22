from concurrent.futures import ThreadPoolExecutor
from multiprocessing.queues import Queue as MultiprocessingQueue
from multiprocessing.synchronize import Event as MultiprocessingEvent
from queue import Empty, ShutDown
from time import sleep, time
from typing import Optional

from .._queues import Message, Queue
from .._utils.logging import worker_logger
from .._utils.pydantic import PydanticModel
from .._utils.retry import handle_failure


class HeartBeat(PydanticModel):
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
            message = self._get_message()

            if message:
                self._heartbeat(message)

                log_extra = {
                    "message_id": message.id,
                    "handler": message.eventide_metadata.handler.name,
                    "attempt": message.eventide_metadata.attempt,
                }

                worker_logger.info(f"Message {message.id} received", extra=log_extra)

                handler = message.eventide_metadata.handler
                with ThreadPoolExecutor(max_workers=1) as executor:
                    start = time()
                    future = executor.submit(handler, message)

                    try:
                        future.result(timeout=handler.timeout)
                    except Exception as exception:
                        end = time()
                        self._heartbeat(None)
                        handle_failure(message, self._queue, exception)

                        if (
                            isinstance(exception, TimeoutError)
                            and end - start >= handler.timeout
                        ):
                            break
                    else:
                        end = time()

                        self._heartbeat(None)
                        self._queue.ack_message(message)

                        worker_logger.info(
                            f"Message {message.id} handling succeeded in "
                            f"{end - start}s",
                            extra={**log_extra, "duration": end - start},
                        )
            else:
                sleep(0.1)

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
