import anyio
import click
from rich.console import Console
from rich.table import Table

from asyncmq.conf import settings

console = Console()

@click.group()
def queue_app():
    """Queue management commands."""
    ...

@queue_app.command("list")
def list_queues():
    """List all queues."""
    backend = settings.backend
    console.print("[bold green]Fetching queues...[/bold green]")

    # Only Memory backend supports this right now.
    if hasattr(backend, "queues"):
        queues = list(backend.queues.keys())
        if queues:
            for queue in queues:
                console.print(f"• {queue}")
        else:
            console.print("[yellow]No queues found.[/yellow]")
    else:
        console.print("[yellow]Queue listing not supported for this backend.[/yellow]")

@queue_app.command("pause")
@click.argument("queue")
def pause_queue(queue: str):
    """Pause a queue."""
    backend = settings.backend
    anyio.run(backend.pause_queue, queue)
    console.print(f"[bold red]Paused queue '{queue}'.[/bold red]")

@queue_app.command("resume")
@click.argument("queue")
def resume_queue(queue: str):
    """Resume a paused queue."""
    backend = settings.backend
    anyio.run(backend.resume_queue, queue)
    console.print(f"[bold green]Resumed queue '{queue}'.[/bold green]")

@queue_app.command("info")
@click.argument("queue")
def info_queue(queue: str):
    """Show info about a queue."""
    backend = settings.backend
    console.print(f"[cyan]Fetching info about queue '{queue}'...[/cyan]\n")

    # Start fetching data
    paused = anyio.run(backend.is_queue_paused, queue)

    waiting_jobs = 0
    delayed_jobs = 0
    dlq_jobs = 0

    # MemoryBackend and RedisBackend have `.queues`, `.delayed`, `.dlqs`
    if hasattr(backend, "queues"):
        waiting_jobs = len(backend.queues.get(queue, []))
    if hasattr(backend, "delayed"):
        delayed_jobs = len(backend.delayed.get(queue, []))
    if hasattr(backend, "dlqs"):
        dlq_jobs = len(backend.dlqs.get(queue, []))

    # Build nice Rich table
    table = Table(title=f"Queue '{queue}' Info", show_header=True, header_style="bold magenta")
    table.add_column("Property", style="cyan", no_wrap=True)
    table.add_column("Value", style="green")

    table.add_row("Paused", "Yes" if paused else "No")
    table.add_row("Waiting Jobs", str(waiting_jobs))
    table.add_row("Delayed Jobs", str(delayed_jobs))
    table.add_row("DLQ Jobs", str(dlq_jobs))

    console.print(table)
