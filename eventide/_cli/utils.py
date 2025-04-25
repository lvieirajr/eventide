from importlib import import_module
from os import getcwd
from sys import modules, path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from .._eventide import Eventide


def resolve_app(app: str, reload: bool = False) -> "Eventide":
    if getcwd() not in path:
        path.insert(0, getcwd())

    module_name, *attrs = app.split(":", 1)
    if reload:
        root = module_name.split(".")[0]

        for sys_module in list(modules):
            if sys_module == root or sys_module.startswith(root + "."):
                del modules[sys_module]

    try:
        module = import_module(module_name)
    except ImportError:
        raise ImportError(f"Module '{module_name}' not found") from None

    for attr in [*attrs, "app", "application"]:
        if hasattr(module, attr):
            return cast("Eventide", getattr(module, attr))

    raise ValueError(f"No Eventide instance found for '{app}'")
