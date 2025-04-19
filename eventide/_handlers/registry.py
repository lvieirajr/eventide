from importlib import import_module
from pathlib import Path
from pkgutil import walk_packages
from sys import path

from .._utils import eventide_logger
from .handler import Handler

handler_registry: set[Handler] = set()


def discover_handlers(paths: list[str]) -> None:
    handler_registry.clear()
    for raw_path in set(paths) or {"."}:
        resolved_path = Path(raw_path).resolve()

        if not resolved_path.exists():
            eventide_logger.debug(f"Path '{resolved_path}' does not exist")
            continue

        base = str(resolved_path.parent if resolved_path.is_file() else resolved_path)
        if base not in path:
            path.insert(0, base)

        if resolved_path.is_file() and resolved_path.suffix == ".py":
            name = resolved_path.stem

            try:
                import_module(name)
            except (ImportError, TypeError):
                eventide_logger.debug(f"Failed to discover handlers from '{name}'")

            continue

        if resolved_path.is_dir():
            init_file = resolved_path / "__init__.py"

            if not init_file.exists():
                eventide_logger.debug(
                    f"Directory '{resolved_path}' is not a Python package",
                )
                continue

            name = resolved_path.name
            try:
                module = import_module(name)
            except (ImportError, TypeError):
                eventide_logger.debug(f"Failed to discover handlers from '{name}'")
                continue

            for _, module_name, is_package in walk_packages(
                module.__path__,
                prefix=module.__name__ + ".",
            ):
                if is_package:
                    continue

                try:
                    import_module(module_name)
                except (ImportError, TypeError):
                    eventide_logger.debug(
                        f"Failed to discover handlers from '{module_name}'",
                    )
