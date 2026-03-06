# Settings Reference

Main settings class: `asyncmq.conf.global_settings.Settings`.

## Core

- `debug: bool = False`
- `logging_level: str = "INFO"`
- `worker_concurrency: int = 1`
- `scan_interval: float = 1.0`
- `heartbeat_ttl: int = 30`
- `tasks: list[str] = []`
- `secret_key: str | None = None`

## Backend Configuration

- `backend` (property): default backend instance (defaults to `RedisBackend()`)

Postgres fields:

- `asyncmq_postgres_backend_url: str | None`
- `asyncmq_postgres_pool_options: dict[str, Any] | None`
- `postgres_jobs_table_name: str = "asyncmq_jobs"`
- `postgres_repeatables_table_name: str = "asyncmq_repeatables"`
- `postgres_cancelled_jobs_table_name: str = "asyncmq_cancelled_jobs"`
- `postgres_workers_heartbeat_table_name: str = "asyncmq_workers_heartbeat"`

MongoDB fields:
- `asyncmq_mongodb_backend_url: str | None`
- `asyncmq_mongodb_database_name: str | None = "asyncmq"`

## Retry/Safety/Runtime

- `enable_stalled_check: bool = False`
- `stalled_check_interval: float = 60.0`
- `stalled_threshold: float = 30.0`
- `sandbox_enabled: bool = False`
- `sandbox_default_timeout: float = 30.0`
- `sandbox_ctx: str | None = "fork"`

## Serialization and Logging

- `json_dumps: Callable[[Any], str]`
- `json_loads: Callable[[str], Any]`
- `json_serializer` (property)
- `logging_config` (property)

## Worker Lifecycle Hooks

- `worker_on_startup: Lifespan | list[Lifespan] | tuple[Lifespan, ...] | None`
- `worker_on_shutdown: Lifespan | list[Lifespan] | tuple[Lifespan, ...] | None`

## Dashboard

- `dashboard_config` property returns `DashboardConfig`

See also:
- [Settings Guide](../features/settings.md)
- [Dashboard](../dashboard/dashboard.md)
