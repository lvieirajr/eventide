from importlib import import_module
from os import getcwd
from pathlib import Path
from sys import modules, path
from typing import TYPE_CHECKING, cast

from typer import Exit, echo

if TYPE_CHECKING:
    from .._eventide import Eventide


def resolve_app(app: str, reload: bool = False) -> "Eventide":
    cwd = Path(getcwd()).resolve()

    if str(cwd) not in path:
        path.insert(0, str(cwd))

    module_name, *attrs = app.split(":", 1)
    if reload:
        for loaded_module in list(modules):
            try:
                module_file = getattr(modules[loaded_module], "__file__", None)

                if module_file and cwd in Path(module_file).resolve().parents:
                    del modules[loaded_module]
            except (AttributeError, TypeError):
                continue

    try:
        module = import_module(module_name)
    except ImportError:
        raise ImportError(f"Module '{module_name}' not found") from None

    for attr in [*attrs, "app", "application"]:
        if hasattr(module, attr):
            return cast("Eventide", getattr(module, attr))

    raise ValueError(f"No Eventide instance found for '{app}'")


def run_with_reload(app: str, command: str) -> None:
    try:
        from watchdog.events import FileSystemEvent, FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError:
        echo("Missing watch dependencies... Install with: pip install eventide[watch]")
        raise Exit(1) from None

    eventide_app, should_reload = resolve_app(app, reload=True), False

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event: FileSystemEvent) -> None:
            nonlocal should_reload

            if str(event.src_path).endswith(".py"):
                eventide_app.shutdown(force=True)
                should_reload = True
                echo("\nChanges detected, reloading...\n")

    observer = Observer()
    observer.schedule(Handler(), str(getcwd()), recursive=True)
    observer.start()

    while True:
        if command == "run":
            eventide_app.run()
        elif command == "cron":
            eventide_app.run_cron()
        else:
            raise ValueError(f"Unknown command: {command}")

        if not should_reload:
            break

        eventide_app, should_reload = resolve_app(app, reload=True), False
