from importlib import import_module
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from .._eventide import Eventide


def resolve_app(app: str) -> "Eventide":
    module_name, *attrs = app.split(":", 1)
    attrs = [*attrs, "app", "application"]

    try:
        module = import_module(module_name)
    except ImportError:
        raise ImportError(f"Module '{module_name}' not found") from None

    for attr in attrs:
        if hasattr(module, attr):
            return cast("Eventide", getattr(module, attr))

    raise ValueError(f"No Eventide instance found for '{app}'")
