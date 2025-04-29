import click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from asyncmq import __version__  # noqa
from asyncmq.cli.utils import get_centered_logo
from asyncmq.conf import settings

console = Console()

@click.group(name="info", invoke_without_command=True)
@click.pass_context
def info_app(ctx):
    """Information commands."""
    if ctx.invoked_subcommand is None:
        _print_info_help()
        click.echo(ctx.get_help())

def _print_info_help():
    text = Text()
    text.append(get_centered_logo(), style="bold cyan")
    text.append("ℹ️  Information Commands\n\n", style="bold cyan")
    text.append("Get information about AsyncMQ version, queues, and backends.\n\n", style="white")
    text.append("Examples:\n", style="bold yellow")
    text.append("  asyncmq info version\n")
    text.append("  asyncmq info backend\n")
    console.print(Panel(text, title="Info CLI", border_style="cyan"))

@info_app.command("version")
def version_command():
    """Show AsyncMQ version."""
    console.print(f"[cyan]AsyncMQ version: {__version__}[/cyan]")

@info_app.command("backend")
def backend():
    """Show current backend configuration."""
    backend_instance = settings.backend
    backend_class = backend_instance.__class__.__name__
    backend_module = backend_instance.__class__.__module__
    console.print(f"[green]Current backend:[/green] [cyan]{backend_module}.{backend_class}[/cyan]")
