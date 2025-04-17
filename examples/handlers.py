from random import uniform
from time import sleep

from eventide import Message, eventide_handler


@eventide_handler("length(body.value) >= `1` && length(body.value) <= `5`")
def handle_1_to_5(message: Message) -> None:
    print(
        "Started handling message with value size of 1-5:",
        f"'{message.body['value']}'",
    )

    sleep(uniform(0, len(message.body["value"]) / 1.0))

    print(
        "Finished handling message with value size of 1-5:",
        f"'{message.body['value']}'",
    )


@eventide_handler("length(body.value) >= `6` && length(body.value) <= `10`")
def handle_6_to_10(message: Message) -> None:
    print(
        "Started handling message with value size of 6-10:",
        f"'{message.body['value']}'",
    )

    sleep(uniform(0, len(message.body["value"]) / 1.0))

    print(
        "Finished handling message with value size of 6-10:",
        f"'{message.body['value']}'",
    )
