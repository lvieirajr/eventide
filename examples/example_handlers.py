from random import uniform
from time import sleep

from eventide import Message, eventide_handler


@eventide_handler("length(body.value) >= `0` && length(body.value) <= `5`")
def handle_0_to_5(message: Message) -> None:
    sleep(uniform(0, len(message.body["value"]) / 100.0))


@eventide_handler("length(body.value) >= `5` && length(body.value) <= `10`")
def handle_5_to_10(message: Message) -> None:
    sleep(uniform(0, len(message.body["value"]) / 100.0))
