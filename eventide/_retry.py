from time import time

from ._handlers import Handler
from ._queues import Message, Queue
from ._utils import worker_logger


def should_retry(handler: Handler, attempt: int, exception: BaseException) -> bool:
    return attempt <= handler.retry_limit and any(
        isinstance(exception, exception_type) for exception_type in handler.retry_for
    )


def handle_failure(
    message: Message,
    queue: Queue[Message],
    exception: Exception,
) -> None:
    handler = message.eventide_metadata.handler
    attempt = message.eventide_metadata.attempt

    if should_retry(handler=handler, attempt=attempt, exception=exception):
        backoff = min(
            handler.retry_max_backoff,
            handler.retry_min_backoff * 2 ** (attempt - 1),
        )

        message.eventide_metadata.attempt = attempt + 1
        message.eventide_metadata.retry_at = time() + backoff

        queue.buffer_retry(message=message)

        worker_logger.warning(
            f"Message {message.id} handling failed with {type(exception).__name__}. "
            f"Retrying in {backoff}s",
            extra={
                "message_id": message.id,
                "handler": handler.name,
                "attempt": attempt,
                "exception": exception,
            },
        )
    else:
        queue.dlq_message(message=message)

        worker_logger.warning(
            f"Message {message.id} handling failed with {type(exception).__name__}",
            extra={
                "message_id": message.id,
                "handler": handler.name,
                "attempt": attempt,
                "exception": exception,
            },
        )
