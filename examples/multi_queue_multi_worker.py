import logging

from eventide import (
    Eventide,
    EventideConfig,
    MockQueueConfig,
    RetryConfig,
    WorkerConfig,
)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    Eventide(
        config=EventideConfig(
            handler_discovery_paths={"./examples"},
            queues=[
                MockQueueConfig(name="queue_1", size=20),
                MockQueueConfig(name="queue_2", size=20),
                MockQueueConfig(name="queue_3", size=20),
            ],
            workers=[
                WorkerConfig(
                    name="worker_1",
                    timeout=1.0,
                    retry_config=RetryConfig(retry_on_timeout=True),
                ),
                WorkerConfig(name="worker_2", timeout=1.0),
                WorkerConfig(name="worker_3", timeout=1.0),
            ],
        ),
    ).run()
