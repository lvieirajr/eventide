from typer import Option

from .eventide import cli
from .utils import resolve_app, run_with_reload


@cli.command("cron")
def cron(
    app: str = Option(..., "--app", "-a", help="App in module:attribute format"),
    reload: bool = Option(False, "--reload", "-r", help="Reload on code changes"),
) -> None:
    resolve_app(app).run_cron() if not reload else run_with_reload(app, "cron")
