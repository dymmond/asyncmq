from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from asyncmq import __version__  # noqa
from asyncmq.backends.base import BaseBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.utils.dashboard import DashboardConfig

if TYPE_CHECKING:
    from asyncmq.logging import LoggingConfig


@dataclass
class Settings:
    """
    Defines a comprehensive set of configuration parameters for the AsyncMQ library.

    This dataclass encapsulates various settings controlling core aspects of
    AsyncMQ's behavior, including debugging modes, logging configuration,
    default backend implementation, database connection details for different
    backends (Postgres, MongoDB), parameters for stalled job recovery,
    sandbox execution settings, worker concurrency limits, and rate limiting
    configurations. It provides a centralized place to manage and access
    these operational monkay.settings.
    """

    debug: bool = False
    """
    Enables debug mode if True.

    Debug mode may activate additional logging, detailed error reporting,
    and potentially other debugging features within the AsyncMQ system.
    Defaults to False.
    """

    logging_level: str = "INFO"
    """
    Specifies the minimum severity level for log messages to be processed.

    Standard logging levels include "DEBUG", "INFO", "WARNING", "ERROR",
    and "CRITICAL". This setting determines the verbosity of the application's
    logging output. Defaults to "INFO".
    """

    backend: BaseBackend = RedisBackend()
    """
    Sets the default backend instance used for queue operations.

    This specifies which storage and message brokering mechanism AsyncMQ
    will use if a specific backend is not explicitly provided for a queue
    or operation. Defaults to an instance of `RedisBackend`.
    """

    version: str = __version__
    """
    Stores the current version string of the AsyncMQ library.

    This attribute holds the version information as defined in the library's
    package metadata. It's read-only and primarily for informational purposes.
    """

    is_logging_setup: bool = False
    """
    Indicates whether the logging system has been initialized.

    This flag is used internally to track the setup status of the logging
    configuration and prevent repeated initialization. Defaults to False.
    """

    jobs_table_schema: str = "asyncmq"
    """
    Specifies the database schema name for Postgres-specific tables.

    When using the Postgres backend, this setting determines the schema
    in which AsyncMQ's job-related tables will be created and accessed.
    Defaults to "asyncmq".
    """

    postgres_jobs_table_name: str = "asyncmq_jobs"
    """
    Defines the name of the table storing job data in the Postgres backend.

    This is the primary table used by the Postgres backend to persist
    information about queued, ongoing, and completed jobs. Defaults to
    "asyncmq_jobs".
    """

    postgres_repeatables_table_name: str = "asyncmq_repeatables"
    """
    Specifies the table name for repeatable job configurations in Postgres.

    This table stores information about jobs scheduled to run at recurring
    intervals when using the Postgres backend. Defaults to
    "asyncmq_repeatables".
    """

    postgres_cancelled_jobs_table_name: str = "asyncmq_cancelled_jobs"
    """
    Sets the table name for cancelled job records in the Postgres backend.

    This table is used to keep track of jobs that have been explicitly
    cancelled when utilizing the Postgres backend. Defaults to
    "asyncmq_cancelled_jobs".
    """
    postgres_workers_heartbeat_table_name: str = "asyncmq_workers_heartbeat"

    asyncmq_postgres_backend_url: str | None = None
    """
    The connection URL (DSN) for the Postgres database.

    This string contains the necessary details (host, port, database name,
    user, password) to establish a connection to the Postgres server used
    by the backend. Can be None if connection details are provided via
    `asyncmq_postgres_pool_options` or elsewhere. Defaults to None.
    """

    asyncmq_postgres_pool_options: dict[str, Any] | None = None
    """
    A dictionary of options for configuring the asyncpg connection pool.

    These options are passed directly to `asyncpg.create_pool` when
    establishing connections to the Postgres database. Allows fine-tuning
    of connection pool behavior. Can be None if default pool options are
    sufficient. Defaults to None.
    """

    asyncmq_mongodb_backend_url: str | None = None
    """
    The connection URL (DSN) for the MongoDB database.

    This string provides the connection details for the MongoDB server when
    using the MongoDB backend. Can be None if MongoDB is not utilized or
    connection details are provided elsewhere. Defaults to None.
    """

    asyncmq_mongodb_database_name: str | None = "asyncmq"
    """
    The name of the database to use within the MongoDB instance.

    Specifies the target database within the MongoDB server where AsyncMQ
    will store its data. Defaults to "asyncmq".
    """

    enable_stalled_check: bool = False
    """
    Activates the stalled job recovery mechanism if True.

    If enabled, a scheduler will periodically check for jobs that have
    been started but have not completed within a defined threshold, marking
    them as failed or re-queuing them. Defaults to False.
    """

    stalled_check_interval: float = 60.0
    """
    The frequency (in seconds) at which the stalled job checker runs.

    This setting determines how often the system scans for potentially
    stalled jobs. Only relevant if `enable_stalled_check` is True. Defaults
    to 60.0 seconds.
    """

    stalled_threshold: float = 30.0
    """
    The time duration (in seconds) after which a job is considered stalled.

    If a job's execution time exceeds this threshold without completion,
    it is flagged as stalled by the checker. Only relevant if
    `enable_stalled_check` is True. Defaults to 30.0 seconds.
    """

    sandbox_enabled: bool = False
    """
    Enables execution of jobs within a sandboxed environment if True.

    Sandboxing can isolate job execution to prevent interference or
    security issues between jobs. Defaults to False.
    """

    sandbox_default_timeout: float = 30.0
    """
    The default maximum execution time (in seconds) for a job in the sandbox.

    Jobs running in the sandbox will be terminated if they exceed this duration.
    Only relevant if `sandbox_enabled` is True. Defaults to 30.0 seconds.
    """

    sandbox_ctx: str | None = "fork"
    """
    Specifies the multiprocessing context method for the sandbox.

    Determines how new processes are created for sandboxed jobs. Possible
    values depend on the operating system but commonly include "fork",
    "spawn", or "forkserver". Only relevant if `sandbox_enabled` is True.
    Defaults to "fork".
    """

    worker_concurrency: int = 1
    """
    The maximum number of jobs a single worker process can execute concurrently.

    This setting controls how many jobs a worker can process in parallel,
    depending on the worker implementation and job types. Defaults to 1.
    """

    scan_interval: float = 1.0
    """
    The frequency (in seconds) at which the scheduler scans for delayed jobs.
    """
    heartbeat_ttl: int = 30

    @property
    def dashboard_config(self) -> DashboardConfig | None:
        return DashboardConfig()

    @property
    def logging_config(self) -> "LoggingConfig" | None:
        """
        Provides the configured logging setup based on current monkay.settings.

        This property dynamically creates and returns an object that adheres
        to the `LoggingConfig` protocol, configured according to the
        `logging_level` attribute. It abstracts the specifics of the logging
        implementation.

        Returns:
            An instance implementing `LoggingConfig` with the specified
            logging level, or None if logging should not be configured
            (though the current implementation always returns a config).
        """
        # Import StandardLoggingConfig locally to avoid potential circular imports
        # if asyncmq.logging depends on asyncmq.conf.monkay.settings.
        from asyncmq.core.utils.logging import StandardLoggingConfig

        # Returns a logging configuration object with the specified level.
        return StandardLoggingConfig(level=self.logging_level)

    def dict(self, exclude_none: bool = False, upper: bool = False) -> dict[str, Any]:
        """
        Converts the Settings object into a dictionary representation.

        Provides a dictionary containing all the configuration settings defined
        in the dataclass. Offers options to exclude None values and transform
        keys to uppercase.

        Args:
            exclude_none: If True, omits key-value pairs where the value is None.
                          Defaults to False.
            upper: If True, converts all dictionary keys to uppercase strings.
                   Defaults to False.

        Returns:
            A dictionary where keys are setting names and values are the
            corresponding setting values.
        """
        original = asdict(self)  # Get the dataclass fields as a dictionary.

        # Handle the case where None values should be included in the output.
        if not exclude_none:
            # Return either the original dictionary or an uppercase-keyed version.
            return {k.upper(): v for k, v in original.items()} if upper else original

        # Handle the case where None values should be excluded from the output.
        # Create a filtered dictionary, then potentially uppercase the keys.
        filtered = {k: v for k, v in original.items() if v is not None}
        return {k.upper(): v for k, v in filtered.items()} if upper else filtered

    def tuple(self, exclude_none: bool = False, upper: bool = False) -> list[tuple[str, Any]]:
        """
        Converts the Settings object into a list of key-value tuples.

        Provides a list of (key, value) tuples representing each configuration
        setting. Allows for excluding tuples with None values and converting
        keys to uppercase within the tuples.

        Args:
            exclude_none: If True, omits tuples where the value is None.
                          Defaults to False.
            upper: If True, converts the key string in each tuple to uppercase.
                   Defaults to False.

        Returns:
            A list of (string, Any) tuples, where each tuple contains a setting
            name and its corresponding value.
        """
        original = asdict(self)  # Get the dataclass fields as a dictionary.

        # Handle the case where None values should be included in the output.
        if not exclude_none:
            # Return a list of items from either the original or uppercase-keyed
            # dictionary.
            return list({k.upper(): v for k, v in original.items()}.items()) if upper else list(original.items())

        # Handle the case where None values should be excluded from the output.
        # Create a filtered list of tuples, then potentially uppercase the keys.
        filtered_tuples = [(k, v) for k, v in original.items() if v is not None]
        return [(k.upper(), v) for k, v in filtered_tuples] if upper else filtered_tuples
