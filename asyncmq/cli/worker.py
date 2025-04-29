import anyio
import click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from asyncmq import __version__  # noqa
from asyncmq.cli.utils import get_centered_logo, print_worker_banner
from asyncmq.conf import settings
from asyncmq.runners import start_worker

console = Console()


@click.group(name="worker", invoke_without_command=True)
@click.pass_context
def worker_app(ctx: click.Context):
    """
    Manages AsyncMQ worker processes.

    This is the main command group for worker-related actions. If no subcommand
    is provided, it prints the help message for the worker commands.

    Args:
        ctx: The Click context object, passed automatically by Click.
    """
    # Check if any subcommand was invoked.
    if ctx.invoked_subcommand is None:
        # If no subcommand, print custom worker help and the standard Click help.
        _print_worker_help()
        click.echo(ctx.get_help())


def _print_worker_help() -> None:
    """
    Prints a custom help message for the worker command group.

    Displays the AsyncMQ logo, a header for worker commands, a brief description,
    and examples of how to use the worker start command. The help message is
    formatted within a Rich Panel.
    """
    text = Text() # Create a Rich Text object to build the formatted output.
    # Add the centered AsyncMQ logo with bold cyan styling.
    text.append(get_centered_logo(), style="bold cyan")
    # Add a header for worker commands with bold cyan styling.
    text.append("⚙️  Worker Commands\n\n", style="bold cyan")
    # Add a descriptive sentence about worker management.
    text.append("Manage AsyncMQ workers to process jobs.\n\n", style="white")
    # Add a section header for examples with bold yellow styling.
    text.append("Examples:\n", style="bold yellow")
    # Add example commands.
    text.append("  asyncmq worker start myqueue --concurrency 2\n")
    text.append("  asyncmq worker start myqueue --concurrency 5\n")
    # Print the text within a Rich Panel with a specific title and border style.
    console.print(Panel(text, title="Worker CLI", border_style="cyan"))


@worker_app.command("start")
@click.argument("queue")
@click.option(
    "--concurrency", default=1, help="Number of concurrent workers."
)
def start_worker_cli(queue: str, concurrency: int):
    """
    Starts an AsyncMQ worker process for a specified queue.

    This command initializes and runs the worker, which listens to the given
    queue and processes messages. It prints a banner with worker details before
    starting. The worker can be stopped by pressing Ctrl+C.

    Args:
        queue: The name of the message queue to listen to. This is a required
               command-line argument.
        concurrency: The number of worker instances to run concurrently.
                     Defaults to 1.
    """
    # Ensure the queue name is not empty.
    if not queue:
        raise click.UsageError("Queue name cannot be empty")

    # Print the worker banner with configuration details.
    print_worker_banner(
        queue, concurrency, settings.backend.__class__.__name__, __version__
    )

    try:
        # Start the worker using anyio's run function.
        # The lambda function wraps the start_worker call to be compatible with anyio.run.
        anyio.run(lambda: start_worker(queue_name=queue, concurrency=concurrency))
    except KeyboardInterrupt:
        # Handle KeyboardInterrupt (Ctrl+C) for graceful shutdown.
        console.print("[yellow]Worker shutdown requested (Ctrl+C). Exiting...[/yellow]")
    except Exception as e:
        # Catch any other exceptions during worker execution.
        console.print(f"[red]Worker crashed: {e}[/red]")
        # Abort the Click process with an error.
        raise click.Abort() from e
