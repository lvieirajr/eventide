from concurrent.futures import ThreadPoolExecutor
from multiprocessing.queues import Queue as MultiprocessingQueue
from multiprocessing.synchronize import Event as MultiprocessingEvent
from queue import Empty, ShutDown
from time import sleep, time
from typing import Callable, Optional

from .._queues import Message, Queue
from .._utils.logging import worker_logger
from .._utils.pydantic import PydanticModel
from .._utils.retry import handle_failure


class HeartBeat(PydanticModel):
    worker_id: int
    timestamp: float
    message: Optional[Message] = None


class Worker:
    worker_id: int

    queue: Queue[Message]

    shutdown: MultiprocessingEvent
    heartbeats: MultiprocessingQueue[HeartBeat]

    on_message_received: Callable[[Message], None]
    on_message_success: Callable[[Message], None]
    on_message_failure: Callable[[Message, Exception], None]

    def __init__(
        self,
        worker_id: int,
        queue: Queue[Message],
        shutdown_event: MultiprocessingEvent,
        heartbeats: MultiprocessingQueue[HeartBeat],
        on_message_received: Callable[[Message], None],
        on_message_success: Callable[[Message], None],
        on_message_failure: Callable[[Message, Exception], None],
    ) -> None:
        self.worker_id = worker_id

        self.queue = queue

        self.shutdown_event = shutdown_event
        self.heartbeats = heartbeats

        self.on_message_received = on_message_received
        self.on_message_success = on_message_success
        self.on_message_failure = on_message_failure

    def run(self) -> None:
        while not self.shutdown_event.is_set():
            message = self._get_message()

            if message:
                self._heartbeat(message)
                self.on_message_received(message)

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
                        handle_failure(message, self.queue, exception)
                        self.on_message_failure(message, exception)

                        if (
                            isinstance(exception, TimeoutError)
                            and end - start >= handler.timeout
                        ):
                            break
                    else:
                        end = time()

                        self._heartbeat(None)
                        self.queue.ack_message(message)
                        self.on_message_success(message)

                        worker_logger.info(
                            f"Message {message.id} handling succeeded in "
                            f"{end - start}s",
                            extra={**log_extra, "duration": end - start},
                        )
            else:
                sleep(0.1)

    def _get_message(self) -> Optional[Message]:
        try:
            return self.queue.get_message()
        except (Empty, ShutDown):
            return None

    def _heartbeat(self, message: Optional[Message] = None) -> None:
        try:
            self.heartbeats.put_nowait(
                HeartBeat(worker_id=self.worker_id, timestamp=time(), message=message),
            )
        except (Empty, ShutDown):
            pass
