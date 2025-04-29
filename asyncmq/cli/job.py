import anyio
import click
from rich.console import Console

from asyncmq.conf import settings  # <-- same as above

console = Console()

@click.group()
def job_app():
    """Job management commands."""
    ...

@job_app.command("inspect")
@click.argument("job_id")
@click.option("--queue", required=True, help="Queue name the job belongs to.")
def inspect_job(job_id: str, queue: str):
    """Inspect job details."""
    backend = settings.backend
    job = anyio.run(backend.job_store.load, queue, job_id)

    if job:
        console.print_json(data=job)
    else:
        console.print(f"[red]Job '{job_id}' not found in queue '{queue}'.[/red]")

@job_app.command("retry")
@click.argument("job_id")
@click.option("--queue", required=True, help="Queue name the job belongs to.")
def retry_job(job_id: str, queue: str):
    """Retry a failed job."""
    backend = settings.backend
    job = anyio.run(backend.job_store.load, queue, job_id)

    if job:
        console.print(f"[green]Retrying job '{job_id}' in queue '{queue}'...[/green]")
        # Remove any old state, enqueue again
        anyio.run(backend.enqueue, queue, job)
    else:
        console.print(f"[red]Job '{job_id}' not found.[/red]")

@job_app.command("remove")
@click.argument("job_id")
@click.option("--queue", required=True, help="Queue name the job belongs to.")
def remove_job(job_id: str, queue: str):
    """Remove a job."""
    backend = settings.backend
    anyio.run(backend.job_store.delete, queue, job_id)
    console.print(f"[bold red]Deleted job '{job_id}' from queue '{queue}'.[/bold red]")
