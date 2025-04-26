from datetime import datetime, timedelta, timezone
from traceback import format_tb
from typing import Any, Callable

from croniter import croniter

from .._utils.logging import cron_logger
from .._utils.pydantic import BaseModel
from .queue import QueueManager


class Cron(BaseModel):
    expression: str
    get_message_body: Callable[[], Any]


class CronManager:
    crons: dict[str, Cron]
    queue_manager: QueueManager
    _last_evaluation: datetime

    def __init__(self, queue_manager: QueueManager) -> None:
        self.crons = {}
        self.queue_manager = queue_manager
        self._last_evaluation = datetime.now(tz=timezone.utc).replace(
            microsecond=0
        ) - timedelta(seconds=1)

    def register(
        self,
        expression: str,
    ) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
        def wrapper(get_message_body: Callable[[], Any]) -> Callable[[], Any]:
            self.crons[
                f"{get_message_body.__module__}.{get_message_body.__qualname__}"
            ] = Cron(
                expression=expression,
                get_message_body=get_message_body,
            )
            return get_message_body

        return wrapper

    def evaluate_crons(self) -> None:
        now = datetime.now(tz=timezone.utc).replace(microsecond=0)

        second = self._last_evaluation + timedelta(seconds=1)
        while second <= now:
            for cron in self.crons.values():
                cron_schedule = croniter(cron.expression, second - timedelta(seconds=1))

                if cron_schedule.get_next(datetime) == second:
                    get_body = cron.get_message_body

                    try:
                        self.queue_manager.queue.send_message(body=get_body())
                    except Exception as exception:
                        cron_logger.warning(
                            f"Failed to send cron message for cron "
                            f"{get_body.__module__}.{get_body.__qualname__}: "
                            f"{exception}",
                            extra={
                                "cron": cron.get_message_body,
                                "expression": cron.expression,
                                "exception": str(exception),
                                "traceback": "".join(
                                    format_tb(exception.__traceback__),
                                ),
                            },
                        )

            second += timedelta(seconds=1)

        self._last_evaluation = now
