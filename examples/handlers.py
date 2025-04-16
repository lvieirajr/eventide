from random import uniform
from time import sleep

from eventide import Message, eventide_handler


@eventide_handler("length(body.value) >= `0` && length(body.value) <= `5`")
def handle_0_to_5(message: Message) -> None:
    print("Handling message with value 0-5:", message)
    sleep(uniform(0, len(message.body["value"]) / 10.0))


@eventide_handler("length(body.value) >= `5` && length(body.value) <= `10`")
def handle_5_to_10(message: Message) -> None:
    print("Handling message with value 5-10:", message)
    sleep(uniform(0, len(message.body["value"]) / 10.0))
