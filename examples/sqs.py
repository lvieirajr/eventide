from logging import INFO, basicConfig
from os import environ
from random import uniform
from time import sleep
from typing import Any

from eventide import Eventide, EventideConfig, Message, SQSQueueConfig

basicConfig(level=INFO)

app = Eventide(
    config=EventideConfig(
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
)


@app.handler("length(body.value) >= `1` && length(body.value) <= `5`")
def handle_1_to_5(message: Message) -> None:
    sleep(uniform(0, len(message.body["value"]) / 3.0))


@app.handler("length(body.value) >= `6` && length(body.value) <= `10`")
def handle_6_to_10(message: Message) -> None:
    sleep(uniform(0, len(message.body["value"]) / 3.0))


@app.handler(lambda message: isinstance(message["body"], str))
def handle_non_json(message: Message) -> None:
    sleep(uniform(0, len(message.body) / 3.0))


@app.cron("* * * * * *")
def cron_message() -> dict[str, Any]:
    return {"value": "test"}
