from importlib import import_module
from pkgutil import walk_packages
from pathlib import Path
from sys import path

from .._utils.logging import get_logger
from .._types import HandlerFuncType, HandlerMatcherType

handler_registry: set[tuple[HandlerMatcherType, HandlerFuncType]] = set()


def discover_handlers(paths: set[str]) -> None:
    logger = get_logger(name="Eventide.HandlerDiscovery")

    for raw_path in {"."}.union(paths):
        resolved_path = Path(raw_path).resolve()

        if not resolved_path.exists():
            logger.debug(f"Path '{resolved_path}' does not exist")
            continue

        base = str(resolved_path.parent if resolved_path.is_file() else resolved_path)
        if base not in path:
            path.insert(0, base)

        if resolved_path.is_file() and resolved_path.suffix == ".py":
            name = resolved_path.stem

            try:
                import_module(name)
            except (ImportError, TypeError):
                logger.debug(f"Failed to discover handlers from '{name}'")

            continue

        if resolved_path.is_dir():
            init_file = resolved_path / "__init__.py"

            if not init_file.exists():
                logger.debug(f"Directory '{resolved_path}' is not a Python package")
                continue

            name = resolved_path.name
            try:
                module = import_module(name)
            except (ImportError, TypeError):
                logger.debug(f"Failed to discover handlers from '{name}'")
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
                    logger.debug(f"Failed to discover handlers from '{module_name}'")
