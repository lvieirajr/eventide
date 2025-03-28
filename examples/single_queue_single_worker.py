import logging

from eventide import Eventide, EventideConfig, MockQueueConfig, WorkerConfig

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    Eventide(
        config=EventideConfig(
            handler_discovery_paths={"./examples"},
            queues=[
                MockQueueConfig(name="queue_1", size=20),
            ],
            workers=[
                WorkerConfig(name="worker_1", timeout=1.0),
            ],
        ),
    ).run()
