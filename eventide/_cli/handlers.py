from typer import Option, echo

from .eventide import cli
from .utils import resolve_app


@cli.command("handlers")
def handlers(
    app: str = Option(
        ...,
        "--app",
        "-a",
        help="App in module:attr format, e.g. main:app",
    ),
) -> None:
    for handler in resolve_app(app).handler_manager.handlers:
        echo(handler.name)
