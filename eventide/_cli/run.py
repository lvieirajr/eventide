from typer import Option

from .eventide import cli
from .utils import resolve_app


@cli.command("run")
def run(
    app: str = Option(
        ...,
        "--app",
        "-a",
        help="App in module:attr format, e.g. main:app",
    ),
) -> None:
    resolve_app(app).run()
