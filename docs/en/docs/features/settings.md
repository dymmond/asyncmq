# Settings

AsyncMQ settings come from a class path defined by `ASYNCMQ_SETTINGS_MODULE`.

Default: `asyncmq.conf.global_settings.Settings`.

## Basic Setup

```python
# myapp/settings.py
from asyncmq.backends.redis import RedisBackend
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    backend = RedisBackend("redis://localhost:6379/0")
    worker_concurrency = 4
    scan_interval = 1.0
    logging_level = "INFO"
```

```bash
export ASYNCMQ_SETTINGS_MODULE=myapp.settings.AppSettings
```

## Important Fields

- `backend`: backend instance used by default when no backend is passed explicitly.
- `worker_concurrency`: default worker concurrency for CLI/`Worker`.
- `scan_interval`: delayed/repeatable scanner interval.
- `enable_stalled_check`: enable heartbeat writes for active jobs.
- `stalled_check_interval`, `stalled_threshold`: values used by stalled recovery scheduler.
- `sandbox_enabled`, `sandbox_default_timeout`, `sandbox_ctx`: subprocess sandbox execution.
- `heartbeat_ttl`: worker heartbeat freshness window (worker listing/visibility).
- `tasks`: module package paths for worker autodiscovery.
- `json_dumps`, `json_loads`: custom JSON serialization hooks.
- `worker_on_startup`, `worker_on_shutdown`: lifecycle hooks.
- `secret_key`: used by dashboard session config.

## Backend-Specific Fields

Postgres:
- `asyncmq_postgres_backend_url`
- `asyncmq_postgres_pool_options`
- `postgres_jobs_table_name`, `postgres_repeatables_table_name`, `postgres_cancelled_jobs_table_name`, `postgres_workers_heartbeat_table_name`

MongoDB:
- `asyncmq_mongodb_backend_url`
- `asyncmq_mongodb_database_name`

## Environment Overrides

Settings fields are read from uppercase environment variables when possible.

Examples:

```bash
export DEBUG=true
export WORKER_CONCURRENCY=8
export SCAN_INTERVAL=0.5
```

For complex objects (like `backend` instances), use a custom settings class instead of env-only overrides.

## Related Pages

- [Custom JSON](custom-json.md)
- [Logging](logging.md)
- [Dashboard](../dashboard/dashboard.md)
- [Settings Reference](../reference/settings-reference.md)
