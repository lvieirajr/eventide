from functools import cached_property
from logging import Logger
from random import choices, randint
from string import printable
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
        max_messages = min(
            self._config.max_messages,
            (self._config.buffer_size or maxsize) - self.size,
        )

        messages = randint(
            min(self._config.min_messages, max_messages),
            max_messages,
        )

        self._logger.info(
            f"Pulled {messages} messages from {type(self).__name__}",
            extra={"config": self._config, "messages": messages},
        )

        return [
            MockMessage(
                id=str(randint(1, maxsize)),
                body={"value": "".join(choices(printable, k=randint(0, 10)))},
            )
            for _ in range(messages)
        ]

    def ack_messages(self) -> None:
        pass

    def dlq_messages(self) -> None:
        pass
