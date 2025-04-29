import anyio
import click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from asyncmq.cli.utils import get_centered_logo
from asyncmq.conf import settings  # <-- same as above

console = Console()

@click.group(name="job", invoke_without_command=True)
@click.pass_context
def job_app(ctx):
    """Job management commands."""
    if ctx.invoked_subcommand is None:
        _print_job_help()
        click.echo(ctx.get_help())

def _print_job_help():
    text = Text()
    text.append(get_centered_logo(), style="bold cyan")
    text.append("🛠️  Job Commands\n\n", style="bold cyan")
    text.append("Inspect, retry, or delete jobs from queues.\n\n", style="white")
    text.append("Examples:\n", style="bold yellow")
    text.append("  asyncmq job inspect jobid123 --queue myqueue\n")
    text.append("  asyncmq job retry jobid123 --queue myqueue\n")
    text.append("  asyncmq job remove jobid123 --queue myqueue\n")
    console.print(Panel(text, title="Job CLI", border_style="cyan"))

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
