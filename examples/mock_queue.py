import logging

from eventide import Eventide, EventideConfig, MockQueueConfig

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    Eventide(
        config=EventideConfig(
            handler_paths=["./examples"],
            queue=MockQueueConfig(
                min_messages=0,
                max_messages=10,
                buffer_size=20,
            ),
            concurrency=2,
            timeout=2.0,
            retry_for=[Exception],
            retry_limit=2,
        ),
    ).run()
