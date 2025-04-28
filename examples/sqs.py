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
            region=environ["SQS_QUEUE_REGION"],
            url=environ["SQS_QUEUE_URL"],
            buffer_size=20,
        ),
        concurrency=4,
        timeout=10.0,
        retry_for=[Exception],
        retry_limit=3,
    ),
)


@app.cron("* * * * * *")
def create_order() -> dict[str, Any]:
    return {
        "event": "order_created",
        "order_id": f"ORD-{int(uniform(1, 1e12))}",
        "amount": round(uniform(1.0, 10000.0), 2),
    }


@app.cron("* * * * * */3")
def cancel_order() -> dict[str, Any]:
    return {
        "event": "order_cancelled",
        "order_id": f"ORD-{int(uniform(1, 1e12))}",
    }


@app.handler("body.event == `order_created`")
def handle_new_order(message: Message) -> None:
    order_id = message.body["order_id"]
    amount = message.body["amount"]

    app.logger.info(f"Processing order {order_id} for ${amount}")

    processing_time = uniform(0.1, 3.0)
    sleep(min(processing_time, 2.0))
    if processing_time > 2.0:
        raise TimeoutError("Payment gateway timed out")

    app.logger.info(f"Order {order_id} processed")


@app.handler("body.event == `order_cancelled`")
def handle_cancel_order(message: Message) -> None:
    order_id = message.body["order_id"]

    app.logger.info(f"Processing refund for cancelled order: {order_id}")

    sleep(uniform(0.1, 1.0))

    app.logger.info(f"Refund for order {order_id} processed")
