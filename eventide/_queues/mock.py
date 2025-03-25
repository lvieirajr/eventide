from random import choices, randint
from string import printable
from sys import maxsize

from pydantic import NonNegativeInt

from .queue import Queue
from .._types import Message, QueueConfig


class MockMessage(Message):
    """
    A message from a Mock queue.
    """

    pass


class MockQueueConfig(QueueConfig):
    """
    Configuration for a mock queue.

    Attributes:
        min_messages (NonNegativeInt): Minimum number of messages to generate.
        max_messages (NonNegativeInt): Maximum number of messages to generate.
    """

    min_messages: NonNegativeInt = 0
    max_messages: NonNegativeInt = 10


@Queue.register(MockQueueConfig)
class MockQueue(Queue[MockMessage]):
    """
    Mocked queue implementation.
    Continuously generates a random number of messages.
    """

    _config: MockQueueConfig

    def pull_messages(self) -> int:
        max_messages = min(
            self._config.max_messages,
            (self._config.size or maxsize) - self.buffer.qsize(),
        )

        messages = randint(
            min(self._config.min_messages, max_messages),
            max_messages,
        )

        self._logger.info(
            f"Pulled {messages} messages from {self}",
            extra={"queue": self._config.name, "messages": messages},
        )

        for _ in range(messages):
            self.put(
                MockMessage(
                    id=str(randint(1, maxsize)),
                    body={"value": "".join(choices(printable, k=randint(0, 10)))},
                ),
            )

        return messages

    def ack_messages(self, messages: list[MockMessage]) -> None:
        pass
