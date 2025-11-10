from __future__ import annotations

import inspect
import json
import os
from functools import cached_property
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from asyncmq import __version__  # noqa
from asyncmq.backends.base import BaseBackend
from asyncmq.core.utils.dashboard import DashboardConfig
from asyncmq.protocols.lifespan import Lifespan

if TYPE_CHECKING:
    from asyncmq.logging import LoggingConfig  # noqa
    from asyncmq.core.json_serializer import JSONSerializer


def safe_get_type_hints(cls: type) -> dict[str, Any]:
    """
    Safely get type hints for a class, handling potential errors.
    This function attempts to retrieve type hints for the given class,
    and if it fails, it prints a warning and returns the class annotations.
    Args:
        cls (type): The class to get type hints for.
    Returns:
        dict[str, Any]: A dictionary of type hints for the class.
    """
    try:
        return get_type_hints(cls, include_extras=True)
    except Exception:
        return cls.__annotations__


class BaseSettings:
    """
    Base of all the settings for any system.
    """

    __type_hints__: dict[str, Any] = None
    __truthy__: set[str] = {"true", "1", "yes", "on", "y"}

    def __init__(self, **kwargs: Any) -> None:
        """
        Initializes the settings by loading environment variables
        and casting them to the appropriate types.
        This method uses type hints from the class attributes to determine
        the expected types of the settings.
        It will look for environment variables with the same name as the class attributes,
        converted to uppercase, and cast them to the specified types.
        If an environment variable is not set, it will use the default value
        defined in the class attributes.
        """

        if kwargs:
            for key, value in kwargs.items():
                setattr(self, key, value)

        for key, typ in self.__type_hints__.items():
            base_type = self._extract_base_type(typ)

            env_value = os.getenv(key.upper(), None)
            if env_value is not None:
                value = self._cast(env_value, base_type)
            else:
                value = getattr(self, key, None)
            setattr(self, key, value)

        # Call post_init if it exists
        self.post_init()

    def __init_subclass__(cls) -> None:
        # the direct class dict has not the key
        if cls.__dict__.get("__type_hints__") is None:
            cls.__type_hints__ = safe_get_type_hints(cls)

    def post_init(self) -> None:
        """
        Post-initialization method that can be overridden by subclasses.
        This method is called after all settings have been initialized.
        """
        ...

    def _extract_base_type(self, typ: Any) -> Any:
        origin = get_origin(typ)
        if origin is Annotated:
            return get_args(typ)[0]
        return typ

    def _cast(self, value: str, typ: type[Any]) -> Any:
        """
        Casts the value to the specified type.
        If the type is `bool`, it checks for common truthy values.
        Raises a ValueError if the value cannot be cast to the type.

        Args:
            value (str): The value to cast.
            typ (type): The type to cast the value to.
        Returns:
            Any: The casted value.
        Raises:
            ValueError: If the value cannot be cast to the specified type.
        """
        try:
            origin = get_origin(typ)
            if origin is Union or origin is UnionType:
                non_none_types = [t for t in get_args(typ) if t is not type(None)]
                if len(non_none_types) == 1:
                    typ = non_none_types[0]
                else:
                    raise ValueError(f"Cannot cast to ambiguous Union type: {typ}")

            if typ is bool or str(typ) == "bool":
                return value.lower() in self.__truthy__
            return typ(value)
        except Exception:
            if get_origin(typ) is Union or get_origin(UnionType):
                type_name = " | ".join(t.__name__ if hasattr(t, "__name__") else str(t) for t in get_args(typ))
            else:
                type_name = getattr(typ, "__name__", str(typ))
            raise ValueError(f"Cannot cast value '{value}' to type '{type_name}'") from None

    def dict(
        self,
        exclude_none: bool = False,
        upper: bool = False,
        exclude: set[str] | None = None,
        include_properties: bool = False,
    ) -> dict[str, Any]:
        """
        Dumps all the settings into a python dictionary.
        """
        result = {}
        exclude = exclude or set()

        for key in self.__type_hints__:
            if key in exclude:
                continue
            value = getattr(self, key, None)
            if exclude_none and value is None:
                continue
            result_key = key.upper() if upper else key
            result[result_key] = value

        if include_properties:
            for name, _ in inspect.getmembers(
                type(self),
                lambda o: isinstance(
                    o,
                    (property, cached_property),
                ),
            ):
                if name in exclude or name in self.__type_hints__:
                    continue
                try:
                    value = getattr(self, name)
                    if exclude_none and value is None:
                        continue
                    result_key = name.upper() if upper else name
                    result[result_key] = value
                except Exception:
                    # Skip properties that raise errors
                    continue

        return result

    def tuple(
        self,
        exclude_none: bool = False,
        upper: bool = False,
        exclude: set[str] | None = None,
        include_properties: bool = False,
    ) -> list[tuple[str, Any]]:
        """
        Dumps all the settings into a tuple.
        """
        return list(
            self.dict(
                exclude_none=exclude_none,
                upper=upper,
                exclude=exclude,
                include_properties=include_properties,
            ).items()
        )


class Settings(BaseSettings):
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

    _backend: BaseBackend | None = None

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

    """
    A list of module paths in which to look for @task-decorated callables.
    E.g. ["myapp.runs.tasks", "myapp.jobs.tasks"].
    """
    tasks: list[str] = []

    json_dumps: Callable[[Any], str] = json.dumps
    """
    Custom JSON serialization function for encoding job data and payloads.

    This function will be used by all backends when serializing data to JSON.
    Useful for handling custom data types like datetime objects, UUID, Decimal,
    or other non-standard JSON types. Defaults to the standard json.dumps.

    Example:
        import json
        from functools import partial
        from uuid import UUID

        def custom_encoder(obj):
            if isinstance(obj, UUID):
                return str(obj)
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        json_dumps = partial(json.dumps, default=custom_encoder)
    """

    json_loads: Callable[[str], Any] = json.loads
    """
    Custom JSON deserialization function for decoding job data and payloads.

    This function will be used by all backends when deserializing JSON data.
    Defaults to the standard json.loads.

    Example:
        import json
        from functools import partial

        json_loads = partial(json.loads, object_hook=custom_decoder)
    """
    worker_on_startup: Lifespan | list[Lifespan] | tuple[Lifespan, ...] | None = None
    """
    One or more hook functions to be executed once when the **worker process starts up**.

    Accepts:
      - a single callable, or
      - a list/tuple of callables.

    Each callable can be sync or async and will be awaited if it returns an awaitable.
    They are executed in the order provided.

    Example:
        async def connect_db(): ...
        def warm_cache(): ...

        worker_on_startup = [connect_db, warm_cache]
    """
    worker_on_shutdown: Lifespan | list[Lifespan] | tuple[Lifespan, ...] | None = None
    """
    One or more hook functions to be executed once when the **worker process is shutting down**.

    Accepts:
      - a single callable, or
      - a list/tuple of callables.

    Each callable can be sync or async and will be awaited if it returns an awaitable.
    They are executed in the order provided.

    Example:
        async def disconnect_db(): ...
        def flush_metrics(): ...

        worker_on_shutdown = (disconnect_db, flush_metrics)
    """
    secret_key: str | None = None
    """
    The secret used for cryptography and for the Dashboard to use sessions.
    """

    @property
    def backend(self) -> BaseBackend:
        """
        Gets the default backend instance used for queue operations.

        This specifies which storage and message brokering mechanism AsyncMQ
        will use if a specific backend is not explicitly provided for a queue
        or operation. Lazily creates a RedisBackend instance if none is set.
        """
        if self._backend is None:
            from asyncmq.backends.redis import RedisBackend

            self._backend = RedisBackend()
        return self._backend

    @backend.setter
    def backend(self, value: BaseBackend) -> None:
        """Sets the default backend instance."""
        self._backend = value

    @property
    def dashboard_config(self) -> DashboardConfig | None:
        """
        Retrieves the default configuration settings for the AsyncMQ management dashboard.

        This property dynamically imports and returns an instance of the `DashboardConfig`
        class, providing access to settings like the authentication backend, template
        directory, and static files location.

        Returns:
            An instance of `DashboardConfig`.
        """
        return DashboardConfig(secret_key=self.secret_key)

    @property
    def logging_config(self) -> "LoggingConfig | None":
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

    @property
    def json_serializer(self) -> "JSONSerializer":
        """
        Provides a JSON serializer instance configured with the current JSON functions.

        This property creates and returns a JSONSerializer instance that properly
        handles both regular functions and partial functions for JSON serialization
        and deserialization. This centralizes the JSON handling logic and ensures
        consistent behavior across all AsyncMQ components.

        Returns:
            A JSONSerializer instance configured with the current json_dumps and
            json_loads functions.
        """
        # Import JSONSerializer locally to avoid potential circular imports
        from asyncmq.core.json_serializer import JSONSerializer

        return JSONSerializer(self.json_dumps, self.json_loads)
