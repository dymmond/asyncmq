import anyio
import click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from asyncmq.backends.base import BaseBackend
from asyncmq.cli.utils import JOBS_LOGO, get_centered_logo, get_print_banner
from asyncmq.conf import monkay

console = Console()


@click.group(name="job", invoke_without_command=True)
@click.pass_context
def job_app(ctx: click.Context) -> None:
    """
    Manages AsyncMQ jobs within queues.

    This is the main command group for job-related actions such as inspecting,
    retrying, or deleting specific jobs. If no subcommand is provided, it prints
    the help message for the job commands.

    Args:
        ctx: The Click context object, passed automatically by Click.
    """
    # Check if any subcommand was invoked.
    if ctx.invoked_subcommand is None:
        # If no subcommand, print custom job help and the standard Click help.
        _print_job_help()
        click.echo(ctx.get_help())


def _print_job_help() -> None:
    """
    Prints a custom help message for the job command group.

    Displays the AsyncMQ logo, a header for job commands, a brief description,
    and examples of how to use the job commands (inspect, retry, remove). The
    help message is formatted within a Rich Panel.
    """
    text = Text()  # Create a Rich Text object to build the formatted output.
    # Add the centered AsyncMQ logo with bold cyan styling.
    text.append(get_centered_logo(), style="bold cyan")
    # Add a header for job commands with bold cyan styling.
    text.append("🛠️  Job Commands\n\n", style="bold cyan")
    # Add a descriptive sentence about job management actions.
    text.append("Inspect, retry, or delete jobs from queues.\n\n", style="white")
    # Add a section header for examples with bold yellow styling.
    text.append("Examples:\n", style="bold yellow")
    # Add example commands for inspecting, retrying, and removing jobs.
    text.append("  asyncmq job inspect jobid123 --queue myqueue\n")
    text.append("  asyncmq job retry jobid123 --queue myqueue\n")
    text.append("  asyncmq job remove jobid123 --queue myqueue\n")
    # Print the text within a Rich Panel with a specific title and border style.
    console.print(Panel(text, title="Job CLI", border_style="cyan"))


@job_app.command("inspect")
@click.argument("job_id")
@click.option("--queue", required=True, help="Queue name the job belongs to.")
def inspect_job(job_id: str, queue: str) -> None:
    """
    Inspects and displays the details of a specific job.

    Retrieves the job data from the backend's job store based on its ID and
    queue name and prints it as a JSON object.

    Args:
        job_id: The unique identifier of the job to inspect.
        queue: The name of the queue where the job is expected to be found.
    """
    backend = monkay.settings.backend  # Get the configured backend instance.

    get_print_banner(JOBS_LOGO, title="AsyncMQ Job Details")
    # Load the job data from the backend's job store using anyio.run.
    job = anyio.run(backend.job_store.load, queue, job_id)

    # Check if the job was found.
    if job:
        # Print the job data formatted as JSON.
        console.print_json(data=job)
    else:
        # If the job was not found, print an error message.
        console.print(f"[red]Job '{job_id}' not found in queue '{queue}'.[/red]")


@job_app.command("retry")
@click.argument("job_id")
@click.option("--queue", required=True, help="Queue name the job belongs to.")
def retry_job(job_id: str, queue: str) -> None:
    """
    Retries a failed or completed job by re-enqueuing it.

    Loads the job from the backend and, if found, removes its old state and
    adds it back to the specified queue for processing.

    Args:
        job_id: The unique identifier of the job to retry.
        queue: The name of the queue where the job is expected to be found.
    """
    backend = monkay.settings.backend  # Get the configured backend instance.

    get_print_banner(JOBS_LOGO, title="AsyncMQ Job Retry")
    # Load the job data from the backend's job store using anyio.run.
    job = anyio.run(backend.job_store.load, queue, job_id)

    # Check if the job was found.
    if job:
        # Print a message indicating the job is being retried.
        console.print(f"[green]Retrying job '{job_id}' in queue '{queue}'...[/green]")
        # Enqueue the job again using anyio.run. This effectively retries it.
        anyio.run(backend.enqueue, queue, job)
    else:
        # If the job was not found, print an error message.
        console.print(f"[red]Job '{job_id}' not found.[/red]")


@job_app.command("remove")
@click.argument("job_id")
@click.option("--queue", required=True, help="Queue name the job belongs to.")
def remove_job(job_id: str, queue: str) -> None:
    """
    Removes a specific job from the backend.

    Deletes the job data from the backend's job store based on its ID and
    queue name.

    Args:
        job_id: The unique identifier of the job to remove.
        queue: The name of the queue where the job is expected to be found.
    """
    backend: BaseBackend = monkay.settings.backend  # Get the configured backend instance.

    get_print_banner(JOBS_LOGO, title="AsyncMQ Job Remove")
    # Delete the job from the backend's job store using anyio.run.
    anyio.run(backend.job_store.delete, queue, job_id)
    # Print a confirmation message.
    console.print(f"[bold red]Deleted job '{job_id}' from queue '{queue}'.[/bold red]")


@job_app.command("cancel-job")
@click.argument("queue")
@click.argument("job_id")
def cli_cancel_job(queue: str, job_id: str | int) -> None:
    """
    Cancels a specific job by its ID in a given queue.

    This command instructs the backend to cancel the job with the specified
    ID in the named queue. The backend is responsible for removing the job
    from relevant queues (waiting, delayed) and marking it as cancelled
    so that workers will skip or stop processing it if it's currently in-flight.
    This command calls the backend's `cancel_job` method.

    Args:
        queue: The name of the queue the job belongs to. This argument is
               required and is passed from the command line.
        job_id: The unique identifier of the job to cancel. This argument is
                required and is passed from the command line. It can be a
                string or an integer depending on how job IDs are managed.
    """
    from asyncmq.queues import Queue

    # Print a banner for the cancel job operation.
    get_print_banner(JOBS_LOGO, title="AsyncMQ Cancel Job")

    # Create a Queue instance for the specified queue name.
    q = Queue(queue)
    # Call the queue's cancel_job method asynchronously using anyio.run,
    # passing the job ID.
    anyio.run(q.cancel_job, job_id)  # type: ignore
    # Print a confirmation message with a no-entry emoji and the job ID.
    console.print(f":no_entry: Cancellation requested for job [bold]{job_id}[/]")


@job_app.command("list")
@click.option("--queue", required=True, help="Queue name to filter jobs")
@click.option("--state", required=True, help="Job state to filter (waiting, active, completed, failed, delayed)")
def list_jobs(queue: str, state: str) -> None:
    from asyncmq.queues import Queue

    """Lists jobs in a specific queue filtered by job state."""
    get_print_banner(JOBS_LOGO, title="AsyncMQ List Jobs")
    # Create a Queue instance for the specified queue name.
    q = Queue(queue)
    jobs = anyio.run(q.list_jobs, state)
    if not jobs:
        console.print(f"[bold yellow]No jobs found in '{queue}' with state '{state}'.[/]")
        return
    for job in jobs:
        console.print(
            f"[green]ID:[/] {job.get('job_id')}  [blue]State:[/] {job.get('status')}  [magenta]Task:[/] {job.get('task_id')}"
        )
