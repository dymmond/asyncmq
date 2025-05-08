from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Annotated, Any, cast

# Assuming typing_extensions is available for Annotated and Doc in this context.
# If not, standard library typing.Annotated (Python 3.9+) would be used,
# and Doc would be from a specific library or convention.
from typing_extensions import Doc

from asyncmq.conf import monkay
from asyncmq.protocols.logging import LoggerProtocol


class LoggerProxy:
    """
    A proxy class that provides access to the real logger instance once it is configured.

    This allows components to use the `logger` object immediately after import,
    even if the actual logging configuration (`setup_logging`) hasn't been called
    yet. The proxy holds a reference to the real logger (`_logger`) and forwards
    all attribute access (method calls like `info()`, `error()`, etc.) to it.
    It uses a reentrant lock (`_lock`) to ensure thread-safe binding of the
    real logger.
    """

    def __init__(self) -> None:
        """
        Initializes the LoggerProxy with a placeholder for the real logger and a lock.
        """
        # Placeholder for the actual logger instance, which will be bound later.
        self._logger: LoggerProtocol | None = None
        # A reentrant lock to synchronize access to the logger binding.
        self._lock: threading.RLock = threading.RLock()

    def bind_logger(self, logger: LoggerProtocol | None) -> None:
        """
        Binds the actual logger instance to the proxy.

        This method is typically called by `setup_logging` after the logging
        system has been configured. Access to the internal logger variable
        is protected by a lock.

        Args:
            logger: The real logger instance (implementing LoggerProtocol) or None.
        """
        # Acquire the lock before modifying the internal logger reference.
        with self._lock:
            # Assign the provided logger instance.
            self._logger = logger

    def __getattr__(self, item: str) -> Any:
        """
        Intercepts attribute access on the proxy and forwards it to the bound logger.

        If the real logger has not yet been bound (is None), it raises a RuntimeError
        indicating that logging setup is incomplete. Access to the internal logger
        variable is protected by a lock.

        Args:
            item: The name of the attribute (e.g., a method name like 'info') being accessed.

        Returns:
            The attribute from the bound logger instance.

        Raises:
            RuntimeError: If the logger has not been bound via `bind_logger`.
        """
        # Acquire the lock before accessing the internal logger reference.
        with self._lock:
            # Check if the real logger has been bound.
            if not self._logger:
                enable_logging()
                # If not bound, raise an error.
                return getattr(self._logger, item)
            # If bound, get and return the requested attribute from the real logger.
            return getattr(self._logger, item)


# Create a global instance of the LoggerProxy. This instance is used throughout
# the application to log messages, regardless of when the real logger is configured.
logger: LoggerProtocol = cast(LoggerProtocol, LoggerProxy())


class LoggingConfig(ABC):
    """
    Abstract base class defining the interface for logging configuration.

    Concrete logging backends (like standard Python logging, loguru, etc.)
    should implement this class to provide custom configuration logic.

    !!! Tip
        You can create your own `LoggingConfig` subclass to use a different
        logging library or apply custom formatting, handlers, etc.

    Attributes:
        __logging_levels__: A class variable listing the valid uppercase string
                            names for standard logging levels.
        level: The minimum logging level (e.g., "DEBUG", "INFO") for the
               root logger. Stored as an uppercase string.
        options: Additional keyword arguments passed during initialization,
                 available for subclass use.
    """

    # Class variable listing valid standard logging level names (uppercase).
    __logging_levels__: list[str] = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    def __init__(
        self,
        level: Annotated[
            str,
            Doc(
                """
                The minimum logging level for the root logger. Must be one of
                the strings listed in `__logging_levels__`. Defaults to "DEBUG".
                """
            ),
        ] = "DEBUG",
        **kwargs: Any,
    ) -> None:
        """
        Initializes the LoggingConfig instance, validating the provided level.

        Args:
            level: The desired logging level as a string. Case-insensitive upon
                   initialization but stored uppercase.
            **kwargs: Additional keyword arguments to store in the `options` attribute.

        Raises:
            AssertionError: If the provided `level` string is not one of the
                            valid standard logging levels.
        """
        # Join valid levels for the assertion error message.
        levels: str = ", ".join(self.__logging_levels__)
        # Assert that the provided level (case-insensitive check) is valid.
        assert (
            level.upper() in self.__logging_levels__
        ), f"'{level}' is not a valid logging level. Available levels: '{levels}'."

        # Store the validated level as uppercase.
        self.level = level.upper()
        # Store any additional keyword arguments.
        self.options = kwargs

    @abstractmethod
    def configure(self) -> None:
        """
        Abstract method to configure the logging monkay.settings.

        Subclasses must implement this method to apply their specific logging
        configuration (e.g., setting up handlers, formatters, loggers) using
        the chosen logging library.
        """
        # This method must be implemented by subclasses.
        raise NotImplementedError("`configure()` must be implemented in subclasses.")

    @abstractmethod
    def get_logger(self) -> Any:
        """
        Abstract method to return the root logger instance after configuration.

        Subclasses must implement this method to provide the configured logger
        instance that will be used by the `LoggerProxy`.

        Returns:
            The logger instance, which is expected to implement `LoggerProtocol`.
        """
        # This method must be implemented by subclasses.
        raise NotImplementedError("`get_logger()` must be implemented in subclasses.")


def setup_logging(logging_config: LoggingConfig | None = None) -> None:
    """
    Sets up the logging system for the application using a provided or default configuration.

    If a custom `LoggingConfig` instance is provided, its `configure` method is called
    to set up the logging system. Otherwise, a default `StandardLoggingConfig` is
    instantiated and used. After configuration, the logger instance obtained from
    `get_logger` is bound to the global `logger` proxy.

    This allows full flexibility to use different logging backends such as
    the standard Python `logging`, `loguru`, `structlog`, or any custom
    implementation based on the `LoggingConfig` interface by providing a
    corresponding `LoggingConfig` subclass instance.

    Args:
        logging_config: An optional instance of a `LoggingConfig` subclass
                        to customize the logging behavior. If not provided, the
                        default `StandardLoggingConfig` will be used.

    Raises:
        ValueError: If the provided `logging_config` is not an instance of
                    the `LoggingConfig` abstract base class.
    """
    from asyncmq.core.utils.logging import StandardLoggingConfig

    # Check if a logging_config was provided and if it's a valid instance.
    if logging_config is not None and not isinstance(logging_config, LoggingConfig):
        # Raise an error if the provided object is not a LoggingConfig instance.
        raise ValueError("`logging_config` must be an instance of LoggingConfig.")

    # Use the provided config or instantiate the default StandardLoggingConfig.
    config = logging_config or StandardLoggingConfig()
    # Call the configure method of the chosen logging config.
    config.configure()

    # Get the logger instance from the configured object.
    _logger = config.get_logger()
    # Bind the obtained logger instance to the global logger proxy.
    logger.bind_logger(_logger)


@lru_cache
def enable_logging() -> None:
    """
    Ensures the application's logging system is configured, running only once.

    This function uses the `@lru_cache` decorator to guarantee that its body
    is executed a maximum of one time across the application's lifespan. Inside,
    it checks the `monkay.settings.is_logging_setup` flag. If the logging system
    has not already been set up according to the settings, it calls the
    `setup_logging` function (imported from `asyncmq.logging`) using the
    logging configuration specified in `monkay.settings.logging_config`.

    The imports are placed inside the function to potentially support lazy
    loading or manage import dependencies.
    """
    # Check if the logging system is already marked as set up in the monkay.settings.
    # This flag is typically managed by the `setup_logging` function itself.
    if not monkay.settings.is_logging_setup:
        # If logging is not set up, call the setup function using the
        # logging configuration from monkay.settings.
        setup_logging(monkay.settings.logging_config)
        monkay.settings.is_logging_setup = True
