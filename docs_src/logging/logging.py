import logging
from typing import Any
import logging.config

from asyncmq.logging import LoggingConfig


class StandardLoggingConfig(LoggingConfig):
    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.config = config if config is not None else self.default_config()

    def default_config(self) -> dict[str, Any]:  # noqa
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                },
            },
            "root": {
                "level": self.level,
                "handlers": ["console"],
            },
        }

    def configure(self) -> None:
        """
        Initialize Python logging for AsyncMQ components.

        1. Sets the root logger level.
        2. Configures a simple console handler with a timestamped format.
        3. Applies the level to the 'asyncmq' logger and all submodules.
        """
        logging.config.dictConfig(self.config)

    def get_logger(self) -> Any:
        return logging.getLogger("asyncmq")
