from random import uniform
from time import sleep

from eventide import Message, eventide_handler


@eventide_handler("length(body.value) >= `1` && length(body.value) <= `5`")
def handle_1_to_5(message: Message) -> None:
    sleep(uniform(0, len(message.body["value"]) / 3.0))


@eventide_handler("length(body.value) >= `6` && length(body.value) <= `10`")
def handle_6_to_10(message: Message) -> None:
    sleep(uniform(0, len(message.body["value"]) / 3.0))
