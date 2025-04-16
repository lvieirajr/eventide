from logging import getLogger, Logger
from typing import Optional


def get_logger(name: str, parent: Optional[Logger] = None) -> Logger:
    if parent:
        return getLogger(f"{parent.name}.{name}")

    return getLogger(name)
