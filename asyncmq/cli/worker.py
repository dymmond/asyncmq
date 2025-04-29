import anyio
import click
from rich.console import Console

from asyncmq import __version__
from asyncmq.cli.utils import print_worker_banner
from asyncmq.conf import settings
from asyncmq.runners import start_worker

console = Console()

@click.group()
def worker_app():
    """Worker management commands."""
    ...

@worker_app.command("start")
@click.argument("queue")
@click.option("--concurrency", default=1, help="Number of concurrent workers.")
def start_worker_cli(queue: str, concurrency: int):
    """Start a worker for a given queue."""
    print_worker_banner(queue, concurrency, settings.backend.__class__.__name__, __version__)

    try:
        anyio.run(lambda: start_worker(queue_name=queue, concurrency=concurrency))
    except KeyboardInterrupt:
        console.print("[yellow]Worker shutdown requested (Ctrl+C). Exiting...[/yellow]")
    except Exception as e:
        console.print(f"[red]Worker crashed: {e}[/red]")
        raise click.Abort() from e
