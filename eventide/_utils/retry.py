from math import ceil
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
    handler, attempt = message.eventide_handler, message.eventide_attempt

    if should_retry(handler=handler, attempt=attempt, exception=exception):
        backoff = min(
            handler.retry_max_backoff,
            handler.retry_min_backoff * 2 ** (attempt - 1),
        )

        queue.retry_message(message=message, backoff=ceil(backoff))

        worker_logger.error(
            f"Message {message.id} handling failed with {type(exception).__name__}. "
            f"Retrying in {backoff}s",
            exc_info=exception,
            extra={
                "message_id": message.id,
                "handler": handler.name,
                "attempt": attempt,
            },
        )
    else:
        queue.dlq_message(message=message)

        worker_logger.error(
            f"Message {message.id} handling failed with {type(exception).__name__}",
            exc_info=exception,
            extra={
                "message_id": message.id,
                "handler": handler.name,
                "attempt": attempt,
            },
        )
