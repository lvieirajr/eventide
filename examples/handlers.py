from random import uniform
from time import sleep

from eventide import Message, eventide_handler


@eventide_handler("length(body.value) >= `0` && length(body.value) <= `4`")
def handle_0_to_5(message: Message) -> None:
    print("Handling message with value 0-4:", message)
    sleep(uniform(0, len(message.body["value"]) / 3.0))


@eventide_handler("length(body.value) >= `5` && length(body.value) <= `9`")
def handle_5_to_10(message: Message) -> None:
    print("Handling message with value 5-9:", message)
    sleep(uniform(0, len(message.body["value"]) / 5.0))
