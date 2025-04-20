from logging import INFO, basicConfig
from os import environ

from eventide import CloudflareQueueConfig, Eventide, EventideConfig

if __name__ == "__main__":
    basicConfig(level=INFO)

    Eventide(
        config=EventideConfig(
            handler_paths=["./examples"],
            queue=CloudflareQueueConfig(
                account_id=environ.get("CLOUDFLARE_ACCOUNT_ID"),
                queue_id=environ.get("CLOUDFLARE_QUEUE_ID"),
                buffer_size=20,
            ),
            concurrency=2,
            timeout=2.0,
            retry_for=[Exception],
            retry_limit=2,
        ),
    ).run()
