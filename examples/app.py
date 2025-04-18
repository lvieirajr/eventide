import logging

from eventide import Eventide, EventideConfig, MockQueueConfig

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    app = Eventide(
        config=EventideConfig(
            handler_paths={"./examples"},
            queue=MockQueueConfig(
                buffer_size=20,
                min_messages=1,
                max_messages=10,
            ),
            concurrency=10,
            timeout=1.0,
            retry_for=[TimeoutError],
            retry_limit=2,
        ),
    )
    app.run()
