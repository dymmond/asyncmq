import anyio
import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from asyncmq.cli.utils import QUEUES_LOGO, get_centered_logo, get_print_banner
from asyncmq.conf import settings

console = Console()


@click.group(name="queue", invoke_without_command=True)
@click.pass_context
def queue_app(ctx: click.Context):
    """
    Manages AsyncMQ queues.

    This is the main command group for queue-related actions like listing,
    pausing, resuming, and viewing information about queues. If no subcommand
    is provided, it prints the help message for the queue commands.

    Args:
        ctx: The Click context object, passed automatically by Click.
    """
    # Check if any subcommand was invoked.
    if ctx.invoked_subcommand is None:
        # If no subcommand, print custom queue help and the standard Click help.
        _print_queue_help()
        click.echo(ctx.get_help())


def _print_queue_help() -> None:
    """
    Prints a custom help message for the queue command group.

    Displays the AsyncMQ logo, a header for queue commands, a brief description,
    and examples of how to use the queue commands. The help message is
    formatted within a Rich Panel.
    """
    text = Text() # Create a Rich Text object to build the formatted output.
    # Add the centered AsyncMQ logo with bold cyan styling.
    text.append(get_centered_logo(), style="bold cyan")
    # Add a header for queue commands with bold cyan styling.
    text.append("📦 Queue Commands\n\n", style="bold cyan")
    # Add a descriptive sentence about queue management.
    text.append("Manage AsyncMQ queues: pause, resume, list jobs.\n\n", style="white")
    # Add a section header for examples with bold yellow styling.
    text.append("Examples:\n", style="bold yellow")
    # Add example commands.
    text.append("  asyncmq queue list\n")
    text.append("  asyncmq queue pause myqueue\n")
    text.append("  asyncmq queue resume myqueue\n")
    # Print the text within a Rich Panel with a specific title and border style.
    console.print(Panel(text, title="Queue CLI", border_style="cyan"))


@queue_app.command("list")
def list_queues() -> None:
    """
    Lists all available queues in the current backend.

    This command retrieves the list of queue names from the configured backend
    and prints them to the console. Note that queue listing support depends
    on the specific backend implementation.
    """
    backend = settings.backend # Get the configured backend instance.
    get_print_banner(QUEUES_LOGO, title="AsyncMQ Queues")
    console.print("[bold green]Fetching queues...[/bold green]")

    # Check if the backend exposes its queues (e.g., MemoryBackend).
    if hasattr(backend, "list_queues"):
        # Get the keys (queue names) from the backend's queues dictionary.
        queues = anyio.run(backend.list_queues)
        if queues:
            # If queues are found, print each one with a bullet point.
            for queue in queues:
                console.print(f"• {queue}")
        else:
            console.print("[yellow]No queues found.[/yellow]")
    else:
        # If the backend does not support listing queues via a 'queues' attribute.
        console.print("[yellow]Queue listing not supported for this backend.[/yellow]")


@queue_app.command("pause")
@click.argument("queue")
def pause_queue(queue: str) -> None:
    """
    Pauses processing for a specific queue.

    When a queue is paused, workers will not pick up new jobs from it until it
    is resumed. This command calls the backend's pause_queue method.

    Args:
        queue: The name of the queue to pause.
    """
    backend = settings.backend # Get the configured backend instance.
    # Call the backend's pause_queue method using anyio.run for async operation.
    anyio.run(backend.pause_queue, queue)

    get_print_banner(QUEUES_LOGO, title="AsyncMQ Queues")
    # Print a confirmation message.
    console.print(f"[bold red]Paused queue '{queue}'.[/bold red]")


@queue_app.command("resume")
@click.argument("queue")
def resume_queue(queue: str) -> None:
    """
    Resumes processing for a specific queue.

    This allows workers to begin picking up jobs from a queue that was previously
    paused. This command calls the backend's resume_queue method.

    Args:
        queue: The name of the queue to resume.
    """
    backend = settings.backend # Get the configured backend instance.
    # Call the backend's resume_queue method using anyio.run for async operation.
    anyio.run(backend.resume_queue, queue)

    get_print_banner(QUEUES_LOGO, title="AsyncMQ Queues")
    # Print a confirmation message.
    console.print(f"[bold green]Resumed queue '{queue}'.[/bold green]")


@queue_app.command("info")
@click.argument("queue")
def info_queue(queue: str) -> None:
    """
    Shows detailed information about a specific queue.

    This includes whether the queue is paused and the number of jobs currently
    in the waiting, delayed, and dead-letter queues (DLQ) for that queue.
    Information availability depends on the backend implementation.

    Args:
        queue: The name of the queue to get information about.
    """
    backend = settings.backend # Get the configured backend instance.

    get_print_banner(QUEUES_LOGO, title="AsyncMQ Queues")
    # Print a message indicating data fetching is in progress.
    console.print(f"[cyan]Fetching info about queue '{queue}'...[/cyan]\n")

    # Start fetching data - check if the queue is paused using anyio.run.
    paused = anyio.run(backend.is_queue_paused, queue)

    # Initialize job counts.
    waiting_jobs = 0
    delayed_jobs = 0
    dlq_jobs = 0

    # Check if the backend exposes attributes containing job queues (common for
    # MemoryBackend and potentially others).
    if hasattr(backend, "queues"):
        # Get the number of waiting jobs for the specific queue.
        waiting_jobs = len(backend.queues.get(queue, []))
    if hasattr(backend, "delayed"):
        # Get the number of delayed jobs for the specific queue.
        delayed_jobs = len(backend.delayed.get(queue, []))
    if hasattr(backend, "dlqs"):
        # Get the number of dead-letter queue jobs for the specific queue.
        dlq_jobs = len(backend.dlqs.get(queue, []))

    # Build a nice Rich table to display the queue information.
    table = Table(
        title=f"Queue '{queue}' Info",
        show_header=True,
        header_style="bold magenta",
    )
    # Add columns to the table.
    table.add_column("Property", style="cyan", no_wrap=True)
    table.add_column("Value", style="green")

    # Add rows with the fetched information.
    table.add_row("Paused", "Yes" if paused else "No")
    table.add_row("Waiting Jobs", str(waiting_jobs))
    table.add_row("Delayed Jobs", str(delayed_jobs))
    table.add_row("DLQ Jobs", str(dlq_jobs))

    # Print the completed table to the console.
    console.print(table)
