import anyio
import click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from asyncmq import __version__
from asyncmq.cli.utils import get_centered_logo, print_worker_banner
from asyncmq.conf import settings
from asyncmq.runners import start_worker

console = Console()

@click.group(name="worker", invoke_without_command=True)
@click.pass_context
def worker_app(ctx):
    """Worker management commands."""
    if ctx.invoked_subcommand is None:
        _print_worker_help()
        click.echo(ctx.get_help())

def _print_worker_help():
    text = Text()
    text.append(get_centered_logo(), style="bold cyan")
    text.append("⚙️  Worker Commands\n\n", style="bold cyan")
    text.append("Manage AsyncMQ workers to process jobs.\n\n", style="white")
    text.append("Examples:\n", style="bold yellow")
    text.append("  asyncmq worker start myqueue --concurrency 2\n")
    text.append("  asyncmq worker start myqueue --concurrency 5\n")
    console.print(Panel(text, title="Worker CLI", border_style="cyan"))

@worker_app.command("start")
@click.argument("queue")
@click.option("--concurrency", default=1, help="Number of concurrent workers.")
def start_worker_cli(queue: str, concurrency: int):
    """Start a worker for a given queue."""
    if not queue:
        raise click.UsageError("Queue name cannot be empty")
    print_worker_banner(queue, concurrency, settings.backend.__class__.__name__, __version__)

    try:
        anyio.run(lambda: start_worker(queue_name=queue, concurrency=concurrency))
    except KeyboardInterrupt:
        console.print("[yellow]Worker shutdown requested (Ctrl+C). Exiting...[/yellow]")
    except Exception as e:
        console.print(f"[red]Worker crashed: {e}[/red]")
        raise click.Abort() from e
