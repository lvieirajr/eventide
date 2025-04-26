from typing import Any, Callable

from .._utils.pydantic import BaseModel
from .queue import QueueManager


class CronJob(BaseModel):
    get_message: Callable[[], Any]
    cron: str


class CronManager:
    crons: set[CronJob]

    queue_manager: QueueManager

    def __init__(self, queue_manager: QueueManager) -> None:
        self.crons = set()

        self.queue_manager = queue_manager

    def register(self, cron: str) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
        def wrapper(get_message: Callable[[], Any]) -> Callable[[], Any]:
            self.crons.add(CronJob(get_message=get_message, cron=cron))
            return get_message

        return wrapper
