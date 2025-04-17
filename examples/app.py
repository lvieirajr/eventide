import logging

from eventide import Eventide, EventideConfig, MockQueueConfig

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    app = Eventide(
        config=EventideConfig(
            handler_paths={"./examples"},
            queue=MockQueueConfig(
                buffer_size=1000,
                min_messages=1,
                max_messages=100,
            ),
            concurrency=20,
            timeout=3.0,
            retry_for=[TimeoutError],
            retry_limit=3,
        ),
    )
    app.run()
