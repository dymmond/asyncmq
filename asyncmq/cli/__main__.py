import click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from sayer import Sayer

from asyncmq.cli.helpers.groups import AsyncMQGroup
from asyncmq.cli.info import info_app
from asyncmq.cli.job import job_app
from asyncmq.cli.queue import queue_app
from asyncmq.cli.utils import get_centered_logo
from asyncmq.cli.worker import worker_app

console = Console()


def _print_main_help() -> None:
    text = Text()
    text.append(get_centered_logo(), style="bold cyan")
    text.append("🚀 AsyncMQ - Powerful Async Job Queue for Python\n\n", style="bold cyan")

    text.append("Manage and inspect your AsyncMQ queues, workers, and jobs.\n\n", style="white")
    text.append("Examples:\n", style="bold yellow")
    text.append("  asyncmq queue list\n")
    text.append("  asyncmq worker start myqueue --concurrency 4\n")
    text.append("  asyncmq job inspect jobid123 --queue myqueue\n")
    text.append("  asyncmq queue pause myqueue\n")
    text.append("  asyncmq info version\n\n")

    text.append("Available backends:\n", style="bold yellow")
    text.append("  - InMemory - Use this for development, in production it is not advised.\n")
    text.append("  - Redis\n")
    text.append("  - Postgres\n")
    text.append("  - MongoDB\n")

    console.print(Panel(text, title="AsyncMQ CLI", border_style="cyan"))


app_cli = Sayer(
    name="asyncmq",
    help="AsyncMQ CLI",
    group_class=AsyncMQGroup,
    invoke_without_command=True,
)


@app_cli.callback(invoke_without_command=True)
def _app_callback(ctx: click.Context) -> None:
    """AsyncMQ CLI"""
    tokens = getattr(ctx, "protected_args", None)
    if tokens is None:
        tokens = ctx.args
    if tokens and tokens[0] in ctx.command.commands:
        return

    # Assuming _print_main_help is defined elsewhere
    try:
        _print_main_help()
    except NameError:
        print("Custom main help placeholder.")
    click.echo(ctx.get_help())


app_cli.add_command(queue_app, name="queue")
app_cli.add_command(job_app, name="job")
app_cli.add_command(worker_app, name="worker")
app_cli.add_command(info_app, name="info")
app = app_cli.cli


def main() -> None:
    app_cli()


if __name__ == "__main__":
    main()
