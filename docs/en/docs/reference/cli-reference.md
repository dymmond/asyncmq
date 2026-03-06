# CLI Reference

## Root

```bash
asyncmq --help
```

Groups: `info`, `queue`, `job`, `worker`.

## `info`

```bash
asyncmq info version
asyncmq info backend
```

## `queue`

```bash
asyncmq queue list
asyncmq queue info <queue>
asyncmq queue pause <queue>
asyncmq queue resume <queue>

asyncmq queue list-delayed <queue>
asyncmq queue remove-delayed <queue> <job_id>

asyncmq queue list-repeatables <queue>
asyncmq queue pause-repeatable <queue> '<job_def_json>'
asyncmq queue resume-repeatable <queue> '<job_def_json>'
```

## `job`

```bash
asyncmq job list --queue <queue> --state <state>
asyncmq job inspect <job_id> --queue <queue>
asyncmq job retry <job_id> --queue <queue>
asyncmq job remove <job_id> --queue <queue>
asyncmq job cancel <queue> <job_id>
```

## `worker`

```bash
asyncmq worker start <queue> --concurrency <n>
asyncmq worker list
asyncmq worker register <worker_id> <queue> --concurrency <n>
asyncmq worker deregister <worker_id>
```

## Settings Resolution

Commands run using the backend/settings from `ASYNCMQ_SETTINGS_MODULE`.
