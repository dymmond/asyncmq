
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()

# ASCII art logo for AsyncMQ. This will be displayed anywhere.
ASYNCMQ_LOGO = r"""
 _______  _______           _        _______  _______  _______
(  ___  )(  ____ \|\     /|( (    /|(  ____ \(       )(  ___  )
| (   ) || (    \/( \   / )|  \  ( || (    \/| () () || (   ) |
| (___) || (_____  \ (_) / |   \ | || |      | || || || |   | |
|  ___  |(_____  )  \   /  | (\ \) || |      | |(_)| || |   | |
| (   ) |      ) |   ) (   | | \   || |      | |   | || | /\| |
| )   ( |/\____) |   | |   | )  \  || (____/\| )   ( || (_\ \ |
|/     \|\_______)   \_/   |/    )_)(_______/|/     \|(____\/_)

""".rstrip()


def get_centered_logo() -> str:
    """
    Centers the ASYNCMQ_LOGO based on the current terminal width.

    Reads the predefined ASCII art logo, determines the current width of the
    console terminal, and then centers each line of the logo within that width.
    The lines are then joined back into a single string.

    Returns:
        str: The centered ASCII art logo as a single multi-line string.
    """
    # Get the current width of the console terminal.
    terminal_width = console.size.width

    # Center each line of the logo manually.
    centered_logo_lines = []
    # Iterate through each line of the raw logo.
    for line in ASYNCMQ_LOGO.splitlines():
        # Center the current line using the terminal width as padding.
        centered_line = line.center(terminal_width)
        centered_logo_lines.append(centered_line)

    # Join the centered lines back into a single string with newlines.
    centered_logo = "\n".join(centered_logo_lines)
    return centered_logo


def print_worker_banner(
    queue: str, concurrency: int, backend_name: str, version: str
) -> None:
    """
    Prints a styled banner to the console for the AsyncMQ worker.

    This function generates a banner using the centered AsyncMQ logo and
    displays key information about the worker's configuration, including
    the version, backend in use, queue being processed, and concurrency level.
    The banner is presented within a Rich Panel for better visual structure.

    Args:
        queue: The name of the message queue the worker is consuming from.
        concurrency: The number of concurrent tasks the worker is running.
        backend_name: The name of the message queue backend being used.
        version: The version of the AsyncMQ worker.
    """
    # Get the centered logo string.
    centered_logo = get_centered_logo()

    # Create a Rich Text object to build the banner content with styles.
    text = Text()
    # Add the centered logo with bold cyan styling.
    text.append(centered_logo, style="bold cyan")
    # Add newline characters for spacing below the logo.
    text.append("\n")
    text.append("\n")
    text.append("\n")
    # Add worker version information with green styling.
    text.append(f"Version: {version}\n", style="green")
    # Add backend name information with green styling.
    text.append(f"Backend: {backend_name}\n", style="green")
    # Add the queue name information with green styling.
    text.append(f"Queue: '{queue}'\n", style="green")
    # Add the concurrency level information with green styling.
    text.append(f"Concurrency: {concurrency}\n", style="green")

    # Print the constructed Text within a styled Rich Panel.
    console.print(Panel(text, title="[b cyan]AsyncMQ Worker", border_style="cyan"))
