import logging

from eventide import Eventide, EventideConfig, SQSQueueConfig

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    Eventide(
        config=EventideConfig(
            handler_paths=["./examples"],
            queue=SQSQueueConfig(
                region="<region>",
                url="<queue_url>",
                buffer_size=20,
            ),
            concurrency=2,
            timeout=2.0,
            retry_for=[Exception],
            retry_limit=2,
        ),
    ).run()
