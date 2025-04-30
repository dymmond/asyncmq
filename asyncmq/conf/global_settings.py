from __future__ import annotations  # Enable postponed evaluation of type hints

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

from asyncmq import __version__  # noqa: F401 # Import package version, ignore unused warning
from asyncmq.backends.base import BaseBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.streams.redis import RedisStream

# Conditionally import LoggingConfig only for type checking purposes.
# This prevents potential import issues or circular dependencies at runtime.
if TYPE_CHECKING:
    from asyncmq.logging import LoggingConfig


@dataclass
class StreamSettings:
    """
    Settings specific to AsyncMQ stream backend configurations.

    This dataclass holds configuration related to stream processing,
    including the default table name for Postgres streams and a mapping
    of available stream backend implementations.
    """

    # For Postgres: The name of the table used for storing stream data.
    postgres_streams_table_name: str = "asyncmq_streams"
    mongo_streams_table_name: str = "asyncmq_streams"

    # A dictionary mapping stream backend names (str) to their corresponding
    # callable factory or class (BaseBackend). Defaults to include the RedisStream.
    stream_backends: dict[str, BaseBackend] = field(
        default_factory=lambda: {
            "redis": RedisStream,
        }
    )

    def add_stream_backend(self, name: str, backend: BaseBackend) -> None:
        """
        Adds or updates a stream backend in the `stream_backends` mapping.

        Args:
            name: The unique name for the stream backend (e.g., "redis", "postgres").
            backend: The callable factory or class for the stream backend.
        """
        self.stream_backends[name] = backend

    def remove_stream_backend(self, name: str) -> None:
        """
        Removes a stream backend from the `stream_backends` mapping.

        Args:
            name: The name of the stream backend to remove.

        Raises:
            KeyError: If the specified backend name is not found.
        """
        del self.stream_backends[name]

    def update_stream_backend(self, name: str, backend: BaseBackend) -> None:
        """
        Updates an existing stream backend in the `stream_backends` mapping.

        This method is functionally identical to `add_stream_backend` as
        dictionary assignment updates existing keys.

        Args:
            name: The name of the stream backend to update.
            backend: The new callable factory or class for the backend.
        """
        self.add_stream_backend(name, backend)


@dataclass
class Settings(StreamSettings):
    """
    Comprehensive configuration settings for the entire AsyncMQ system.

    Inherits stream-specific settings from `StreamSettings`. This dataclass
    defines various configuration options for debugging, logging, default
    backend, database connections (Postgres, MongoDB), stalled job recovery,
    sandbox execution, worker concurrency, and rate limiting.
    """

    # Debug mode flag. If True, enables debugging features and potentially
    # changes logging level.
    debug: bool = False
    # The logging level for the application (e.g., "INFO", "DEBUG", "WARNING").
    logging_level: str = "INFO"
    # The default backend instance to use for queue operations if not
    # specified otherwise. Defaults to an instance of RedisBackend.
    # Note: The type hint is for the class, but the default is an instance
    # based on the original code's logic.
    backend: BaseBackend = RedisBackend()
    # The version string of the AsyncMQ library.
    version: str = __version__
    # Flag indicating if logging has been set up.
    is_logging_setup: bool = False

    # For Postgres backend: The schema name for Postgres tables.
    jobs_table_schema: str = "asyncmq"
    # For Postgres backend: The name of the table used for storing job data.
    postgres_jobs_table_name: str = "asyncmq_jobs"
    # For Postgres backend: The connection URL (DSN) for the Postgres database.
    # Can be None if connection string is provided directly to functions.
    asyncmq_postgres_backend_url: str | None = None
    # For Postgres backend: A dictionary of options to pass to asyncpg.create_pool.
    # Can be None if default pool options are sufficient or options are
    # provided directly to functions.
    asyncmq_postgres_pool_options: dict[str, Any] | None = None

    # For MongoDB backend: The connection URL for the MongoDB database.
    # Can be None if MongoDB is not used or connection string is provided elsewhere.
    asyncmq_mongodb_backend_url: str | None = None
    # For MongoDB backend: The name of the database to use in MongoDB.
    # Defaults to "asyncmq".
    asyncmq_mongodb_database_name: str | None = "asyncmq"

    # For stalled recovery scheduler: The interval (in seconds) between checks
    # for stalled jobs.
    stalled_check_interval: float = 60.0
    # For stalled recovery scheduler: The threshold (in seconds) after which
    # a job is considered stalled.
    stalled_threshold: float = 30.0

    # For the sandbox settings usage: Flag to enable sandbox execution.
    sandbox_enabled: bool = False
    # For the sandbox settings usage: Default timeout (in seconds) for sandbox processes.
    sandbox_default_timeout: float = 30.0
    # For the sandbox settings usage: The context method for starting processes
    # ("fork", "spawn", or "forkserver").
    sandbox_ctx: str | None = "fork"

    # How many jobs to run in parallel per worker process.
    worker_concurrency: int = 10

    # Rate-limit configuration for RateLimiter. A dictionary where keys are
    # queue names and values are dictionaries specifying rate limit parameters
    # like "max_calls" and "period". Can be None if no rate limiting is configured.
    rate_limit_config: dict[str, dict[str, int]] | None = None

    @property
    def logging_config(self) -> "LoggingConfig" | None:
        """
        Returns the logging configuration based on the current settings.

        This property dynamically creates and returns a logging configuration
        object. Currently, it uses `StandardLoggingConfig` and sets the level
        based on the `logging_level` attribute. In more complex scenarios,
        it could return different configurations or None.

        Returns:
            An instance of a class implementing `LoggingConfig` (specifically
            `StandardLoggingConfig` in this implementation) with the configured
            logging level, or None if logging should be disabled.
        """
        # Import StandardLoggingConfig locally to avoid potential circular imports
        # if asyncmq.logging depends on asyncmq.conf.settings.
        from asyncmq.core.utils.logging import StandardLoggingConfig

        # Return a StandardLoggingConfig instance configured with the specified level.
        return StandardLoggingConfig(level=self.logging_level)

    def dict(self, exclude_none: bool = False, upper: bool = False) -> dict[str, Any]:
        """
        Dumps all the settings into a Python dictionary.

        This method converts the dataclass instance into a dictionary,
        with options to exclude None values and convert keys to uppercase.

        Args:
            exclude_none: If True, keys with a value of None are excluded
                          from the resulting dictionary. Defaults to False.
            upper: If True, converts all keys in the resulting dictionary
                   to uppercase. Defaults to False.

        Returns:
            A dictionary containing the settings data.
        """
        # Get the dictionary representation of the dataclass fields using asdict.
        original = asdict(self)

        # Handle the case where None values should be included.
        if not exclude_none:
            # If keys should not be upper-cased, return the original dictionary.
            if not upper:
                return original
            # If keys should be upper-cased, create a new dictionary with upper keys.
            return {k.upper(): v for k, v in original.items()}

        # Handle the case where None values should be excluded.
        # If keys should not be upper-cased, return a filtered dictionary.
        if not upper:
            return {k: v for k, v in original.items() if v is not None}
        # If keys should be upper-cased, filter and create a new dictionary
        # with upper keys.
        return {k.upper(): v for k, v in original.items() if v is not None}

    def tuple(self, exclude_none: bool = False, upper: bool = False) -> list[tuple[str, Any]]:
        """
        Dumps all the settings into a list of key-value tuples.

        This method converts the dataclass instance into a list of (key, value)
        tuples, with options to exclude tuples with None values and convert
        keys to uppercase.

        Args:
            exclude_none: If True, tuples where the value is None are excluded
                          from the resulting list. Defaults to False.
            upper: If True, converts the keys in the resulting tuples
                   to uppercase. Defaults to False.

        Returns:
            A list of (key, value) tuples containing the settings data.
        """
        # Get the dictionary representation of the dataclass fields using asdict.
        original = asdict(self)

        # Handle the case where None values should be included.
        if not exclude_none:
            # If keys should not be upper-cased, return a list of original items.
            if not upper:
                return list(original.items())
            # If keys should be upper-cased, create a dictionary with upper keys
            # and return a list of its items.
            return list({k.upper(): v for k, v in original.items()}.items())

        # Handle the case where None values should be excluded.
        # If keys should not be upper-cased, return a filtered list of tuples.
        if not upper:
            return [(k, v) for k, v in original.items() if v is not None]
        # If keys should be upper-cased, filter and create a list of tuples
        # with upper keys.
        return [(k.upper(), v) for k, v in original.items() if v is not None]
