from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()

def print_worker_banner(queue: str, concurrency: int, backend_name: str, version: str) -> None:
    logo = r"""
 _______  _______           _        _______  _______  _______
(  ___  )(  ____ \|\     /|( (    /|(  ____ \(       )(  ___  )
| (   ) || (    \/( \   / )|  \  ( || (    \/| () () || (   ) |
| (___) || (_____  \ (_) / |   \ | || |      | || || || |   | |
|  ___  |(_____  )  \   /  | (\ \) || |      | |(_)| || |   | |
| (   ) |      ) |   ) (   | | \   || |      | |   | || | /\| |
| )   ( |/\____) |   | |   | )  \  || (____/\| )   ( || (_\ \ |
|/     \|\_______)   \_/   |/    )_)(_______/|/     \|(____\/_)

""".rstrip()
    # Get terminal width
    terminal_width = console.size.width

    # Center each line manually
    centered_logo_lines = []
    for line in logo.splitlines():
        centered_line = line.center(terminal_width)
        centered_logo_lines.append(centered_line)

    centered_logo = "\n".join(centered_logo_lines)

    text = Text()
    text.append(centered_logo, style="bold cyan")
    text.append("\n")
    text.append("\n")
    text.append("\n")
    text.append(f"Version: {version}\n", style="green")
    text.append(f"Backend: {backend_name}\n", style="green")
    text.append(f"Queue: '{queue}'\n", style="green")
    text.append(f"Concurrency: {concurrency}\n", style="green")

    console.print(Panel(text, title="[b cyan]AsyncMQ Worker", border_style="cyan"))
