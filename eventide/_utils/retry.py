from time import time
from traceback import format_tb
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

    exception_type = type(exception).__name__
    if should_retry(handler=handler, attempt=attempt, exception=exception):
        backoff = min(
            handler.retry_max_backoff,
            handler.retry_min_backoff * 2 ** (attempt - 1),
        )

        message.eventide_metadata.attempt = attempt + 1
        message.eventide_metadata.retry_at = time() + backoff

        queue.put_retry_message(message=message)

        worker_logger.warning(
            f"Message {message.id} handling failed with {exception_type}. Retrying in "
            f"{backoff}s",
            extra={
                "message_id": message.id,
                "handler": handler.name,
                "attempt": attempt,
                "exception": str(exception),
                "traceback": "".join(format_tb(exception.__traceback__)),
            },
        )
    else:
        worker_logger.error(
            f"Message {message.id} handling failed with {exception_type}",
            extra={
                "message_id": message.id,
                "handler": handler.name,
                "attempt": attempt,
                "exception": str(exception),
                "traceback": "".join(format_tb(exception.__traceback__)),
            },
        )
