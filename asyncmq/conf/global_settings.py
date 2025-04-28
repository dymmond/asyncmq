from dataclasses import asdict, dataclass
from typing import Any

from asyncmq import __version__
from asyncmq.backends.base import BaseBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.logging import LoggingConfig, StandardLoggingConfig


@dataclass
class _BaseSettings:
    """
    Base class for asyncmq settings dataclasses.

    Provides utility methods for dumping settings into dictionary or list
    of tuples formats, with options to exclude None values and convert keys
    to uppercase.
    """
    is_logging_setup: bool = False

    def dict(self, exclude_none: bool = False, upper: bool = False) -> dict[str, Any]:
        """
        Dumps all the settings into a Python dictionary.

        Args:
            exclude_none: If True, keys with a value of None are excluded
                          from the resulting dictionary. Defaults to False.
            upper: If True, converts all keys in the resulting dictionary
                   to uppercase. Defaults to False.

        Returns:
            A dictionary containing the settings data.
        """
        # Get the dictionary representation of the dataclass fields.
        original = asdict(self)

        # Handle the case where None values should be included.
        if not exclude_none:
            # If keys should not be upper-cased, return the original dict.
            if not upper:
                return original
            # If keys should be upper-cased, return a new dict with upper keys.
            return {k.upper(): v for k, v in original.items()}

        # Handle the case where None values should be excluded.
        # If keys should not be upper-cased, return a filtered dict.
        if not upper:
            return {k: v for k, v in original.items() if v is not None}
        # If keys should be upper-cased, return a filtered dict with upper keys.
        return {k.upper(): v for k, v in original.items() if v is not None}

    def tuple(self, exclude_none: bool = False, upper: bool = False) -> list[tuple[str, Any]]:
        """
        Dumps all the settings into a list of key-value tuples.

        Args:
            exclude_none: If True, tuples with a value of None are excluded
                          from the resulting list. Defaults to False.
            upper: If True, converts the keys in the resulting tuples
                   to uppercase. Defaults to False.

        Returns:
            A list of (key, value) tuples containing the settings data.
        """
        # Get the dictionary representation of the dataclass fields.
        original = asdict(self)

        # Handle the case where None values should be included.
        if not exclude_none:
            # If keys should not be upper-cased, return a list of original items.
            if not upper:
                return list(original.items())
            # If keys should be upper-cased, convert and return a list of items.
            return list({k.upper(): v for k, v in original.items()}.items())

        # Handle the case where None values should be excluded.
        # If keys should not be upper-cased, return a filtered list of tuples.
        if not upper:
            return [(k, v) for k, v in original.items() if v is not None]
        # If keys should be upper-cased, filter, convert, and return a list of tuples.
        return [(k.upper(), v) for k, v in original.items() if v is not None]


@dataclass
class Settings(_BaseSettings):
    """
    Configuration settings for the asyncmq system.

    Inherits utility methods from `_BaseSettings`.

    Attributes:
        debug: A boolean indicating whether the application is running in
               debug mode. Defaults to False.
        backend: The default backend instance to use for queue operations.
                 Defaults to an instance of `RedisBackend`.
                 NOTE: The type hint is for the class, but the default is
                 an instance based on the original code logic.
        version: A string representing the version of AsyncMQ. Defaults to
                 the package's `__version__`.
    """

    debug: bool = False
    backend: type[BaseBackend] = RedisBackend() # Keeping original logic default
    version: str = __version__

    @property
    def logging_config(self) -> LoggingConfig | None:
        """
        Returns the logging configuration based on the current settings.

        If `debug` is True, the logging level is set to "DEBUG", otherwise it's
        set to "INFO". It returns an instance of `StandardLoggingConfig`.
        A more advanced implementation might return different `LoggingConfig`
        subclasses or None based on other settings.

        Returns:
            An instance of `StandardLoggingConfig` with the appropriate level,
            or potentially None if logging should be disabled (though this
            implementation always returns a config).
        """
        # Determine the logging level based on the debug setting.
        level = "DEBUG" if self.debug else "INFO"
        # Return a StandardLoggingConfig instance with the determined level.
        return StandardLoggingConfig(level=level)
