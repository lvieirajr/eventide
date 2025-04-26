from logging import INFO
from typing import Any, Literal

from pydantic import Field, NonNegativeInt, PositiveFloat, PositiveInt, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .._queues import QueueConfig


class EventideConfig(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",
        validate_default=False,
    )

    queue: QueueConfig

    min_pull_interval: PositiveFloat = 1.0
    max_pull_interval: PositiveFloat = 10.0

    concurrency: PositiveInt = 1

    timeout: PositiveFloat = 60.0

    retry_for: list[type[Exception]] = Field(default_factory=list)
    retry_min_backoff: PositiveFloat = 1.0
    retry_max_backoff: PositiveFloat = 60.0
    retry_limit: NonNegativeInt = 3

    log_level: Literal[0, 10, 20, 30, 40, 50] = INFO

    @field_validator("log_level", mode="before")
    @classmethod
    def parse_log_level(cls, value: Any) -> Any:
        try:
            return int(value)
        except (ValueError, TypeError):
            return value
