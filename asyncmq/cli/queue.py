from rich.console import Console
from rich.table import Table

from asyncmq.cli.sync import AsyncTyper
from asyncmq.conf import settings

queue_app = AsyncTyper(name="queue", help="Manage queues")
console = Console()

@queue_app.command("list")
async def list_queues():
    """
    List all queues.
    """
    queues = await settings.backend.list_queues()
    if not queues:
        console.print("[yellow]No queues found.[/yellow]")
        return

    table = Table(title="Queues")
    table.add_column("Name", style="cyan", no_wrap=True)
    for name in queues:
        table.add_row(name)
    console.print(table)


@queue_app.command("pause")
async def pause_queue(name: str):
    """
    Pause a queue.
    """
    await settings.backend.pause_queue(name)
    console.print(f"[bold green]Queue '{name}' paused successfully.[/bold green]")


@queue_app.command("resume")
async def resume_queue(name: str):
    """
    Resume a paused queue.
    """
    await settings.backend.resume_queue(name)
    console.print(f"[bold green]Queue '{name}' resumed successfully.[/bold green]")


@queue_app.command("info")
async def queue_info(name: str):
    """
    Show stats about a queue (waiting, delayed, failed jobs).
    """
    stats = await settings.backend.queue_stats(name)
    if not stats:
        console.print(f"[yellow]Queue '{name}' not found or empty.[/yellow]")
        return

    table = Table(title=f"Queue Info: {name}")
    table.add_column("Status", style="magenta")
    table.add_column("Count", style="cyan")
    for key, value in stats.items():
        table.add_row(key, str(value))
    console.print(table)
