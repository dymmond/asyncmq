# Troubleshooting

## Worker Starts but Jobs Never Run

Check:

1. Queue name matches task queue name.
2. Task module is imported (or configured in `settings.tasks` for autodiscovery).
3. Backend in producer and worker is the same environment/config.

Useful commands:

```bash
asyncmq info backend
asyncmq queue list
asyncmq job list --queue <queue> --state waiting
```

## Jobs Stay in `waiting`

Common causes:
- queue paused (`asyncmq queue info <queue>`)
- no worker consuming that queue
- unresolved dependencies (`depends_on` parents not completed)

## Delayed Jobs Never Fire

Check `scan_interval` and worker process health.
Delayed jobs are moved by `delayed_job_scanner`, so a worker loop must be running.

## Stalled Jobs Not Recovered

`enable_stalled_check=True` only enables heartbeat recording.
You also need a recovery scheduler loop:

```python
from asyncmq.core.stalled import stalled_recovery_scheduler
await stalled_recovery_scheduler()
```

## Postgres Backend Errors on Missing Tables

Initialize schema before running workers/producers:

```python
import anyio
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend

anyio.run(install_or_drop_postgres_backend)
```

## Dashboard Redirects to Login Unexpectedly

Check:

- `AsyncMQAdmin(enable_login=True, backend=...)` has a valid backend
- session middleware is enabled (`include_session=True` or host app equivalent)
- mount path/prefix is consistent with configured dashboard prefix

## CLI Uses Wrong Backend

Ensure the same settings module is exported in your shell/session:

```bash
export ASYNCMQ_SETTINGS_MODULE=myapp.settings.AppSettings
asyncmq info backend
```
