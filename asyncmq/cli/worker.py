import signal
import time

import anyio
import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from asyncmq import __version__  # noqa
from asyncmq.cli.utils import (
    WORKERS_LOGO,
    get_centered_logo,
    get_print_banner,
    print_worker_banner,
)
from asyncmq.conf import monkay
from asyncmq.logging import logger

console = Console()


@click.group(name="worker", invoke_without_command=True)
@click.pass_context
def worker_app(ctx: click.Context) -> None:
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
    text = Text()  # Create a Rich Text object to build the formatted output.
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
@click.option("--concurrency", required=False, help="Number of concurrent workers.")
def start_worker(queue: str, concurrency: int | str | None = None) -> None:
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
    from asyncmq.runners import start_worker

    # Ensure the queue name is not empty.
    if not queue:
        raise click.UsageError("Queue name cannot be empty")

    concurrency = concurrency or monkay.settings.worker_concurrency
    if isinstance(concurrency, str):
        concurrency = int(concurrency)

    # Print the worker banner with configuration details.
    print_worker_banner(queue, concurrency, monkay.settings.backend.__class__.__name__, __version__)
    logger_level = getattr(monkay.settings, "logging_level", "info")
    log = getattr(logger, logger_level.lower())
    try:
        # Start the worker using anyio's run function.
        # The lambda function wraps the start_worker call to be compatible with anyio.run.
        log(f"Starting worker for queue '{queue}' with concurrency {concurrency}")
        anyio.run(lambda: start_worker(queue_name=queue, concurrency=concurrency))
    except anyio.get_cancelled_exc_class():
        # Handle KeyboardInterrupt (Ctrl+C) for graceful shutdown.
        console.print("[yellow]Worker shutdown requested (Ctrl+C). Exiting...[/yellow]")
    except Exception as e:
        # Catch any other exceptions during worker execution.
        console.print(f"[red]Worker crashed: {e}[/red]")
        # Abort the Click process with an error.
        raise click.Abort() from e


async def signal_handler(scope: anyio.CancelScope) -> None:
    """Listens for signals and cancels the task group."""
    with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                console.print("\n[yellow]KeyboardInterrupt received (Ctrl+C).[/yellow]")
            else:
                console.print(f"\n[yellow]Received signal {signum}.[/yellow]")
            scope.cancel()  # Cancel the task group to initiate shutdown
            return  # Exit the signal handler task


@worker_app.command("list-workers")
def list_workers() -> None:
    """
    List all currently registered workers.

    Retrieves the list of workers from the backend and displays them
    in a table, including their ID, queue, concurrency, and last heartbeat timestamp.
    """
    get_print_banner(WORKERS_LOGO, title="AsyncMQ List Workers")
    backend = monkay.settings.backend
    workers = anyio.run(backend.list_workers)
    table = Table(title="Workers")
    table.add_column("Worker ID", style="green")
    table.add_column("Queue", style="magenta")
    table.add_column("Concurrency", justify="right")
    table.add_column("Last Heartbeat", style="yellow")
    for w in workers:
        heartbeat_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(w.heartbeat))
        table.add_row(w.id, w.queue, str(w.concurrency), heartbeat_str)
    console.print(table)


@worker_app.command("register-worker")
@click.argument("worker_id")
@click.argument("queue")
@click.option("--concurrency", default=1, help="Concurrency level for the worker.")
def register_worker(worker_id: str, queue: str, concurrency: int) -> None:
    """
    Register or update a worker's heartbeat in the backend.

    This command registers a worker with a specific ID to a given queue
    and sets its concurrency level. It updates the worker's timestamp
    in the backend to indicate it is active.

    Args:
        worker_id: The unique identifier for the worker.
        queue: The name of the queue the worker will process tasks from.
        concurrency: The maximum number of tasks the worker can process concurrently.
                     Defaults to 1.
    """
    get_print_banner(WORKERS_LOGO, title="AsyncMQ Register Workers")
    backend = monkay.settings.backend
    timestamp = time.time()
    anyio.run(
        lambda: backend.register_worker(
            worker_id=worker_id,
            queue=queue,
            concurrency=concurrency,
            timestamp=timestamp,
        )
    )
    console.print(
        f":white_check_mark: Worker [bold]{worker_id}[/] registered on queue [bold]{queue}[/] with concurrency {concurrency}."
    )


@worker_app.command("deregister-worker")
@click.argument("worker_id")
def deregister_worker(worker_id: str) -> None:
    """
    Deregister a worker from the backend.

    This command removes a worker's entry from the backend, effectively
    deregistering it.

    Args:
        worker_id: The unique identifier of the worker to deregister.
    """
    get_print_banner(WORKERS_LOGO, title="AsyncMQ Deregister Workers")
    backend = monkay.settings.backend
    anyio.run(lambda: backend.deregister_worker(worker_id))
    console.print(f":white_check_mark: Worker [bold]{worker_id}[/] deregistered.")
