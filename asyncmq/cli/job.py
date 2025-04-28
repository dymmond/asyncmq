import json

import typer
from rich.console import Console
from rich.table import Table

from asyncmq.cli.sync import AsyncTyper
from asyncmq.conf import settings

job_app = AsyncTyper(name="job", help="Manage jobs")
console = Console()


@job_app.command("list")
async def list_jobs(queue: str):
    """
    List all jobs in a queue.
    """
    jobs = await settings.backend.list_jobs(queue)
    if not jobs:
        console.print(f"[yellow]No jobs found in queue '{queue}'.[/yellow]")
        return

    table = Table(title=f"Jobs in Queue: {queue}")
    table.add_column("Job ID", style="cyan", no_wrap=True)
    table.add_column("Status", style="magenta")
    table.add_column("Task", style="green")
    for job in jobs:
        job_id = job.get("id", "-")
        status = job.get("status", "-")
        task = job.get("task_id", "-")
        table.add_row(str(job_id), str(status), str(task))
    console.print(table)


@job_app.command("inspect")
async def inspect_job(job_id: str, queue: str):
    """
    Show detailed info about a specific job.
    """
    job = await settings.backend.job_store.load(queue, job_id)
    if not job:
        console.print(f"[red]Job '{job_id}' not found in queue '{queue}'.[/red]")
        raise typer.Exit(1)

    console.print_json(json.dumps(job))


@job_app.command("retry")
async def retry_job(job_id: str, queue: str):
    """
    Retry a failed job manually.
    """
    success = await settings.backend.retry_job(queue, job_id)
    if success:
        console.print(f"[bold green]Job '{job_id}' retried successfully.[/bold green]")
    else:
        console.print(f"[red]Failed to retry job '{job_id}'.[/red]")


@job_app.command("remove")
async def remove_job(job_id: str, queue: str):
    """
    Remove a job from the system.
    """
    success = await settings.backend.remove_job(queue, job_id)
    if success:
        console.print(f"[bold green]Job '{job_id}' removed successfully.[/bold green]")
    else:
        console.print(f"[red]Failed to remove job '{job_id}'.[/red]")
