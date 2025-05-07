import click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from asyncmq import __version__  # noqa
from asyncmq.cli.utils import INFO_LOGO, get_centered_logo, get_print_banner
from asyncmq.conf import monkay

console = Console()


@click.group(name="info", invoke_without_command=True)
@click.pass_context
def info_app(ctx: click.Context) -> None:
    """
    Provides information about the AsyncMQ installation and configuration.

    This is the main command group for information-related actions such as
    displaying the version or the configured backend. If no subcommand is
    provided, it prints the help message for the info commands.

    Args:
        ctx: The Click context object, passed automatically by Click.
    """
    # Check if any subcommand was invoked.
    if ctx.invoked_subcommand is None:
        # If no subcommand, print custom info help and the standard Click help.
        _print_info_help()
        click.echo(ctx.get_help())


def _print_info_help() -> None:
    """
    Prints a custom help message for the info command group.

    Displays the AsyncMQ logo, a header for information commands, a brief
    description, and examples of how to use the info commands (version,
    backend). The help message is formatted within a Rich Panel.
    """
    text = Text()  # Create a Rich Text object to build the formatted output.
    # Add the centered AsyncMQ logo with bold cyan styling.
    text.append(get_centered_logo(), style="bold cyan")
    # Add a header for information commands with bold cyan styling.
    text.append("ℹ️  Information Commands\n\n", style="bold cyan")
    # Add a descriptive sentence about the type of information available.
    text.append(
        "Get information about AsyncMQ version, queues, and backends.\n\n",
        style="white",
    )
    # Add a section header for examples with bold yellow styling.
    text.append("Examples:\n", style="bold yellow")
    # Add example commands for getting version and backend info.
    text.append("  asyncmq info version\n")
    text.append("  asyncmq info backend\n")
    # Print the text within a Rich Panel with a specific title and border style.
    console.print(Panel(text, title="Info CLI", border_style="cyan"))


@info_app.command("version")
def version_command() -> None:
    """
    Displays the current installed version of AsyncMQ.

    Retrieves the version string from the package metadata and prints it
    to the console.
    """
    # Print the AsyncMQ version string. The __version__ variable is imported
    # directly from the asyncmq package.
    get_print_banner(INFO_LOGO, "AsyncMQ Version")
    console.print(f"[cyan]AsyncMQ version: {__version__}[/cyan]")


@info_app.command("backend")
def backend() -> None:
    """
    Displays the currently configured backend for AsyncMQ.

    Shows the full import path (module and class name) of the backend
    implementation being used according to the application monkay.settings.
    """
    # Get the configured backend instance from monkay.settings.
    backend_instance = monkay.settings.backend
    # Get the class name of the backend instance.
    backend_class = backend_instance.__class__.__name__
    # Get the module path where the backend class is defined.
    backend_module = backend_instance.__class__.__module__

    get_print_banner(INFO_LOGO, "AsyncMQ Backend")
    # Print the formatted string showing the current backend module and class.
    console.print(f"[green]Current backend:[/green] [cyan]{backend_module}.{backend_class}[/cyan]")
