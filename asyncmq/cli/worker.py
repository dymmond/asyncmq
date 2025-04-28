import typer
from rich.console import Console

from asyncmq.cli.sync import AsyncTyper
from asyncmq.runners import start_worker

worker_app = AsyncTyper(name="worker", help="Manage workers")  # <- use AsyncTyper here
console = Console()


@worker_app.command("start")
async def start_worker_cli(
    queue: str,
    concurrency: int = typer.Option(1, help="Number of concurrent workers"),
):
    """
    Start a worker to process jobs from a queue.
    """
    console.print(f"[bold green]Starting worker for queue '{queue}' with concurrency {concurrency}...[/bold green]")

    try:
        await start_worker(queue_name=queue, concurrency=concurrency)
    except KeyboardInterrupt:
        console.print("[yellow]Worker shutdown requested (Ctrl+C). Exiting...[/yellow]")
    except Exception as e:
        console.print(f"[red]Worker crashed: {e}[/red]")
        raise typer.Exit(1) from e
