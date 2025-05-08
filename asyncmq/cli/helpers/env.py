import sys
from pathlib import Path


class AsyncMQEnv:
    """
    Loads an arbitraty application into the object
    and returns the App.
    """

    path: str | None = None
    command_path: str | None = None

    def enable_settings(self) -> None:
        # Adds the current path where the command is being invoked
        # To the system path
        cwd = Path().cwd()
        command_path = str(cwd)
        if command_path not in sys.path:
            sys.path.append(command_path)
