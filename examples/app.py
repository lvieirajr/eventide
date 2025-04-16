import logging

from eventide import Eventide, EventideConfig, MockQueueConfig

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    app = Eventide(
        config=EventideConfig(
            handler_paths={"./examples"},
            queue=MockQueueConfig(buffer_size=1000),
            concurrency=1,
            timeout=1.0,
        ),
    )
    app.run()
