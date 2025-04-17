from functools import cached_property
from logging import Logger
from random import choices, randint
from string import ascii_letters, digits
from sys import maxsize

from pydantic import NonNegativeInt, PositiveInt

from .queue import Queue
from .._types import Message, QueueConfig
from .._utils.logging import get_logger


class MockMessage(Message):
    pass


class MockQueueConfig(QueueConfig):
    min_messages: NonNegativeInt = 0
    max_messages: PositiveInt = 10


@Queue.register(MockQueueConfig)
class MockQueue(Queue[MockMessage]):
    _config: MockQueueConfig

    @cached_property
    def _logger(self) -> Logger:
        return get_logger(name="mock", parent=super()._logger)

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

        self._logger.info(
            f"Pulled {message_count} messages from Mock Queue",
            extra={
                "config": self._config.model_dump(logging=True),
                "messages": message_count,
            },
        )

        return [
            MockMessage(
                id=str(randint(1, maxsize)),
                body={
                    "value": "".join(choices(ascii_letters + digits, k=randint(1, 10))),
                },
            )
            for _ in range(message_count)
        ]

    def ack_message(self, message: MockMessage) -> None:
        self._logger.debug(f"Acknowledged message {message.id}")

    def dlq_message(self, message: MockMessage) -> None:
        self._logger.debug(f"Sent message {message.id} to the DLQ")
