from logging import INFO, basicConfig
from os import environ

from eventide import Eventide, EventideConfig, SQSQueueConfig

if __name__ == "__main__":
    basicConfig(level=INFO)

    Eventide(
        config=EventideConfig(
            handler_paths=["./examples"],
            queue=SQSQueueConfig(
                region=environ.get("SQS_QUEUE_REGION"),
                url=environ.get("SQS_QUEUE_URL"),
                buffer_size=20,
            ),
            concurrency=2,
            timeout=2.0,
            retry_for=[Exception],
            retry_limit=2,
        ),
    ).run()
