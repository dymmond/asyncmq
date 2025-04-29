import logging
import logging.config
from typing import Any

from asyncmq.logging import LoggingConfig


class StandardLoggingConfig(LoggingConfig):
    """
    A concrete implementation of `LoggingConfig` using the standard Python `logging` module.

    Provides a default dictionary-based configuration for the standard logging
    module with a console handler and basic formatter.
    """

    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        """
        Initializes the StandardLoggingConfig, merging default and provided configuration.

        Args:
            config: An optional dictionary conforming to Python's logging
                    dictionary schema. If provided, it overrides or extends the
                    default configuration. Defaults to None, using only the
                    default configuration.
            **kwargs: Additional keyword arguments passed to the base `LoggingConfig`
                      initializer (e.g., `level`).
        """
        # Initialize the base LoggingConfig with level and other kwargs.
        super().__init__(**kwargs)
        # Store the provided config or the default configuration.
        self.config: dict[str, Any] = config or self.default_config()

    def default_config(self) -> dict[str, Any]:
        """
        Provides a default logging configuration dictionary for the standard `logging` module.

        Sets up a basic formatter and a console handler. The root logger's level
        is set based on `self.level`.

        Returns:
            A dictionary representing the default logging configuration.
        """
        # Return a standard Python logging dictionary configuration.
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
        Configures the standard Python logging system using the stored configuration dictionary.
        """
        # Apply the dictionary configuration to the standard logging module.
        logging.config.dictConfig(self.config)

    def get_logger(self) -> Any:
        """
        Returns the root logger instance configured by `logging.config.dictConfig`.

        Returns:
            The standard Python logger instance named "asyncmq".
        """
        # Get and return a logger instance by name.
        return logging.getLogger("asyncmq")
