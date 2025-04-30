from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from asyncmq import __version__
from asyncmq.backends.base import BaseBackend
from asyncmq.backends.redis import RedisBackend

if TYPE_CHECKING:
    from asyncmq.logging import LoggingConfig


@dataclass
class Settings:
    """
    Configuration settings for the asyncmq system.

    Inherits utility methods from `_PostgresSettings`.

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
    logging_level: str = "INFO"
    backend: BaseBackend = RedisBackend()  # Keeping original logic default
    version: str = __version__
    is_logging_setup: bool = False

    # For Postgres backend
    jobs_table_schema: str = "asyncmq"
    jobs_table_name: str = "asyncmq_jobs"
    asyncmq_postgres_backend_url: str | None = None

    # For MongoDB backend
    asyncmq_mongodb_backend_url: str | None = None
    asyncmq_mongodb_database_name: str | None = "asyncmq"

    # For stalled recovery scheduler
    stalled_check_interval: float = 60.0
    stalled_threshold: float = 30.0

    # For the sandbox settings usage
    sandbox_enabled: bool = False
    sandbox_default_timeout: float = 30.0 # seconds

    # How many jobs to run in parallel per worker process
    worker_concurrency: int = 10

    # Rate-limit configuration for RateLimiter (e.g. {"my-queue": {"max_calls": 100, "period": 60}})
    rate_limit_config: dict[str, dict[str, int]] | None = None

    @property
    def logging_config(self) -> "LoggingConfig" | None:
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
        from asyncmq.core.utils.logging import StandardLoggingConfig

        # Return a StandardLoggingConfig instance with the determined level.
        return StandardLoggingConfig(level=self.logging_level)

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
