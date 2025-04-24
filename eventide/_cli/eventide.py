from typer import Typer

cli: Typer = Typer(help="Eventide")

from .handlers import handlers  # noqa: E402, F401
from .run import run  # noqa: E402, F401
from .watch import watch  # noqa: E402, F401
