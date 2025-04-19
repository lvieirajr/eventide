import logging

from eventide import CloudflareQueueConfig, Eventide, EventideConfig

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    Eventide(
        config=EventideConfig(
            handler_paths=["./examples"],
            queue=CloudflareQueueConfig(
                account_id="<account_id>",
                queue_id="<queue_id>",
                buffer_size=20,
            ),
            concurrency=2,
            timeout=2.0,
            retry_for=[Exception],
            retry_limit=2,
        ),
    ).run()
