from typer import Exit, Option, echo

from .eventide import cli
from .utils import resolve_app


@cli.command("watch")
def watch(
    app: str = Option(
        ...,
        "--app",
        "-a",
        help="App in module:attr format, e.g. main:app",
    ),
) -> None:
    try:
        from watchdog.events import FileSystemEvent, FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError:
        echo("Missing watch dependencies... Install with: pip install eventide[watch]")
        raise Exit(1) from None

    eventide_app, should_reload = resolve_app(app), True

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event: FileSystemEvent) -> None:
            nonlocal should_reload

            if str(event.src_path).endswith(".py"):
                should_reload = True
                eventide_app.shutdown(force=True)

                echo("\nChanges detected, reloading...\n")

    observer = Observer()
    observer.schedule(Handler(), ".", recursive=True)
    observer.start()

    while True:
        should_reload = False
        eventide_app.run()

        if not should_reload:
            break
