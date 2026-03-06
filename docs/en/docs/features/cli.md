# CLI Overview

AsyncMQ ships with the `asyncmq` command.

```bash
asyncmq --help
```

Command groups:

- `info`
- `queue`
- `job`
- `worker`

## Quick Examples

```bash
asyncmq info version
asyncmq info backend

asyncmq queue list
asyncmq queue info default
asyncmq queue pause default
asyncmq queue resume default

asyncmq job list --queue default --state waiting
asyncmq job inspect <job_id> --queue default
asyncmq job retry <job_id> --queue default
asyncmq job remove <job_id> --queue default
asyncmq job cancel default <job_id>

asyncmq worker start default --concurrency 4
asyncmq worker list
asyncmq worker register <worker_id> default --concurrency 2
asyncmq worker deregister <worker_id>
```

## Settings Resolution

CLI uses `ASYNCMQ_SETTINGS_MODULE` the same way as application code.

```bash
export ASYNCMQ_SETTINGS_MODULE=myapp.settings.AppSettings
```

For full command reference and options, see [CLI Reference](../reference/cli-reference.md).
