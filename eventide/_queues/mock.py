from random import choices, randint
from string import ascii_letters, digits
from sys import maxsize
from uuid import uuid4

from pydantic import NonNegativeInt, PositiveInt

from .._utils import queue_logger
from .queue import Message, Queue, QueueConfig


class MockMessage(Message):
    pass


class MockQueueConfig(QueueConfig):
    min_messages: NonNegativeInt = 0
    max_messages: PositiveInt = 10


@Queue.register(MockQueueConfig)
class MockQueue(Queue[MockMessage]):
    _config: MockQueueConfig

    def pull_messages(self) -> list[MockMessage]:
        with self._size.get_lock():
            max_messages = min(
                self._config.max_messages,
                (self._config.buffer_size or maxsize) - self._size.value,
            )

        message_count = randint(
            min(self._config.min_messages, max_messages),
            max_messages,
        )

        queue_logger.debug(
            f"Pulled {message_count} messages from Mock Queue",
            extra={"config": self._config.model_dump(), "messages": message_count},
        )

        return [
            MockMessage(
                id=str(uuid4()),
                body={
                    "value": "".join(choices(ascii_letters + digits, k=randint(1, 10))),
                },
            )
            for _ in range(message_count)
        ]

    def ack_message(self, message: MockMessage) -> None:
        queue_logger.debug(f"Acknowledged message {message.id}")

    def dlq_message(self, message: MockMessage) -> None:
        queue_logger.debug(f"Sent message {message.id} to the DLQ")
