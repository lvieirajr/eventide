from logging import getLogger, Logger
from typing import Optional


def get_logger(parent: Optional[Logger] = None, name: Optional[str] = None) -> Logger:
    if parent:
        if name:
            return getLogger(f"{parent.name}.{name}")

        raise ValueError("Child logger needs a name")

    if name:
        return getLogger(f"Eventide.{name}")

    return getLogger("Eventide")
