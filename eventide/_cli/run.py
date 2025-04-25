from typer import Exit, Option, echo

from .eventide import cli
from .utils import resolve_app


@cli.command("run")
def run(
    app: str = Option(..., "--app", "-a", help="App in module:attribute format"),
    reload: bool = Option(False, "--reload", "-r", help="Reload on code changes"),
) -> None:
    if not reload:
        resolve_app(app, reload=False).run()
        return

    try:
        from watchdog.events import FileSystemEvent, FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError:
        echo("Missing watch dependencies... Install with: pip install eventide[watch]")
        raise Exit(1) from None

    eventide_app, should_reload = resolve_app(app, reload=True), False

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event: FileSystemEvent) -> None:
            nonlocal should_reload, eventide_app

            if str(event.src_path).endswith(".py"):
                eventide_app.shutdown(force=True)
                eventide_app = resolve_app(app, reload=True)
                should_reload = True
                echo("\nChanges detected, reloading...\n")

    observer = Observer()
    observer.schedule(Handler(), ".", recursive=True)
    observer.start()

    while True:
        eventide_app.run()

        if not should_reload:
            break

        eventide_app, should_reload = resolve_app(app, reload=True), False
