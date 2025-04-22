from time import time
from typing import TYPE_CHECKING

from .logging import worker_logger

if TYPE_CHECKING:
    from .._handlers import Handler
    from .._queues import Message, Queue


def should_retry(handler: "Handler", attempt: int, exception: BaseException) -> bool:
    return attempt <= handler.retry_limit and any(
        isinstance(exception, exception_type) for exception_type in handler.retry_for
    )


def handle_failure(
    message: "Message",
    queue: "Queue[Message]",
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

        queue.put_retry_message(message=message)

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
        worker_logger.warning(
            f"Message {message.id} handling failed with {type(exception).__name__}",
            extra={
                "message_id": message.id,
                "handler": handler.name,
                "attempt": attempt,
                "exception": exception,
            },
        )
