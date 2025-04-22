from random import choices, randint
from string import ascii_letters, digits
from uuid import uuid4

from pydantic import NonNegativeInt, PositiveInt

from .._utils.logging import queue_logger
from .queue import Message, Queue, QueueConfig


class MockMessage(Message):
    pass


class MockQueueConfig(QueueConfig):
    min_messages: NonNegativeInt = 0
    max_messages: PositiveInt = 10


@Queue.register(MockQueueConfig)
class MockQueue(Queue[MockMessage]):
    _config: MockQueueConfig

    @property
    def max_messages_per_pull(self) -> int:
        return self._config.max_messages

    def pull_messages(self) -> list[MockMessage]:
        message_count = randint(self._config.min_messages, self._config.max_messages)

        queue_logger.debug(f"Pulled {message_count} messages from Mock Queue")

        return [
            self._build_json_message() if index % 2 == 0 else self._build_str_message()
            for index in range(message_count)
        ]

    def ack_message(self, message: MockMessage) -> None:
        queue_logger.debug(f"Acknowledged message {message.id}")

    def _build_json_message(self) -> MockMessage:
        return MockMessage(
            id=str(uuid4()),
            body={
                "value": "".join(choices(ascii_letters + digits, k=randint(1, 10))),
            },
        )

    def _build_str_message(self) -> MockMessage:
        return MockMessage(
            id=str(uuid4()),
            body="".join(choices(ascii_letters + digits, k=randint(1, 10))),
        )
