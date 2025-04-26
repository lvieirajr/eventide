from importlib import import_module
from os import getcwd
from sys import modules, path
from typing import TYPE_CHECKING, cast

from typer import Exit, echo

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
    observer.schedule(Handler(), ".", recursive=True)
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
