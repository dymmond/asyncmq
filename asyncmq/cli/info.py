import click
from rich.console import Console

from asyncmq import __version__  # noqa

console = Console()

@click.group()
def info_app():
    """Information commands."""
    ...

@info_app.command("version")
def version_command():
    """Show AsyncMQ version."""
    console.print(f"[cyan]AsyncMQ version {__version__}[/cyan]")
