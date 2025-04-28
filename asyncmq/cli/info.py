from rich.console import Console

from asyncmq import __version__
from asyncmq.cli.sync import AsyncTyper

info_app = AsyncTyper(name="info", help="Information commands")
console = Console()

VERSION = __version__


@info_app.command("version")
def version():
    """
    Show AsyncMQ CLI version.
    """
    console.print(f"[bold green]AsyncMQ CLI Version: {VERSION}[/bold green]")
