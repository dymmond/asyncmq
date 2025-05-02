import logging
from typing import Any

from asyncmq.logging import LoggingConfig


class StandardLoggingConfig(LoggingConfig):

    def configure(self) -> None:
        """
        Initialize Python logging for AsyncMQ components.

        1. Sets the root logger level.
        2. Configures a simple console handler with a timestamped format.
        3. Applies the level to the 'asyncmq' logger and all submodules.
        """
        # Convert string to logging constant
        lvl = getattr(logging, self.level.upper(), logging.INFO)
        # Basic console format
        fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        logging.basicConfig(level=lvl, format=fmt)

        # Ensure AsyncMQ loggers inherit this level
        logging.getLogger("asyncmq").setLevel(lvl)

    def get_logger(self) -> Any:
        return logging.getLogger("asyncmq")
