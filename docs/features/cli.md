# AsyncMQ CLI

The `asyncmq` CLI is your command-line control center for managing queues, jobs, workers, and backend workers,
all wrapped in a splashy Rich banner and powered by Click.

---

## Installation & Entry Point

Install AsyncMQ into your Python environment:

```bash
pip install asyncmq
```

Invoke the CLI with:

```bash
asyncmq [group|command] [subcommand] [options]
```

Show global help:

```bash
asyncmq --help
```

!!! Tip
You can also run it as a module:

```bash
python -m asyncmq
```

### Custom Settings Module

By default, AsyncMQ loads configuration from `asyncmq.conf.global_settings`.

To override this, set the `ASYNCMQ_SETTINGS_MODULE` environment variable:

```bash
export ASYNCMQ_SETTINGS_MODULE=your.project.settings
```

This allows you to point the CLI at your own `Settings` dataclass (with custom backends, rate limits, sandbox options, etc.).

---

## Top-Level Commands

These commands are invoked directly on the `asyncmq` root without a sub-group.

### `list-queues`

List all queues known to the configured backend.

```bash
asyncmq queue list
```

### `list-workers`

List all registered workers, their queue, concurrency level, and last heartbeat.

```bash
asyncmq worker list
```

### `register-worker <worker_id> <queue> [--concurrency N]`

Register or update a worker's heartbeat and concurrency on a specific queue.

```bash
asyncmq worker register worker42 myqueue --concurrency 10
```

### `deregister-worker <worker_id>`

Deregister (remove) a worker from the registry.

```bash
asyncmq worker deregister worker42
```

---

## Command Groups Overview

In addition to the top-level commands above, the CLI is organized into four primary groups:

* **`queue`**: Manage and inspect queues (list, pause/resume, delayed, repeatables)
* **`job`**: Inspect, list, retry, remove, or cancel individual jobs
* **`worker`**: Start worker processes to consume queues
* **`info`**: Display current backend implementation and configuration details

Run `asyncmq <group> --help` to see usage for each group.

---

## `asyncmq info`

Displays the configured backend’s Python import path (and settings module).

```bash
asyncmq info
```

No additional flags or subcommands.

---

## `asyncmq queue` Commands

Manage and inspect your queues:

### `list`

List all queues known to the backend.

```bash
asyncmq queue list
```

### `info <queue>`

Show stats for a specific queue:

* Paused state
* Counts of waiting, delayed, failed jobs

```bash
asyncmq queue info myqueue
```

### `pause <queue>`

Stop workers from pulling new jobs off a queue.

```bash
asyncmq queue pause myqueue
```

### `resume <queue>`

Restart processing of a paused queue.

```bash
asyncmq queue resume myqueue
```

### `list-delayed <queue>`

Show all jobs scheduled for future execution.

```bash
asyncmq queue list-delayed myqueue
```

### `remove-delayed <queue> <job_id>`

Cancel a delayed job by ID.

```bash
asyncmq queue remove-delayed myqueue 123e4567
```

### `list-repeatables <queue>`

List all repeatable job definitions on a queue.

```bash
asyncmq queue list-repeatables myqueue
```

### `pause-repeatable <queue> '<job_def_json>'`

Pause a specific repeatable definition (given as its JSON dict).

```bash
asyncmq queue pause-repeatable myqueue '{"task_id":"app.heartbeat","every":60}'
```

### `resume-repeatable <queue> '<job_def_json>'`

Un-pause and reschedule a paused repeatable.

```bash
asyncmq queue resume-repeatable myqueue '{"task_id":"app.heartbeat","every":60}'
```

!!! Warning
**JSON Strings** must be wrapped in single quotes in most shells to prevent interpretation.

---

## `asyncmq job` Commands

Operations on individual jobs:

### `list --queue <q> --state <state>`

List jobs filtered by state (`waiting`, `active`, `completed`, `failed`, `delayed`).

```bash
asyncmq job list --queue myqueue --state failed
```

### `inspect <job_id> --queue <q>`

Dump full job data (args, status, timestamps) as JSON.

```bash
asyncmq job inspect 123e4567 --queue myqueue
```

### `retry <job_id> --queue <q>`

Re-enqueue a completed or failed job.

```bash
asyncmq job retry 123e4567 --queue myqueue
```

### `remove <job_id> --queue <q>`

Delete job metadata completely.

```bash
asyncmq job remove 123e4567 --queue myqueue
```

### `cancel <job_id> --queue <q>`

Signal in-flight job to stop and remove it from future processing.

```bash
asyncmq job cancel 123e4567 --queue myqueue
```

---

## `asyncmq worker` Commands

Control worker processes consuming queues:

### `start <queue> [--concurrency N]`

Launch a worker that processes up to `N` jobs concurrently (default 1).

```bash
asyncmq worker start myqueue --concurrency 5
```

* Press **Ctrl+C** to gracefully shut down.
* Banner shows version, backend, queue, and concurrency.

---

## Putting It All Together

* Start a worker for "emails" queue, 3 at a time:

```bash
asyncmq worker start emails --concurrency 3
```

* List all queues:

```bash
asyncmq queue list
```

* Register a new worker heartbeat:

```bash
asyncmq worker register worker42 emails --concurrency 3
```

* List workers:

```bash
asyncmq worker list
```

* Deregister a worker:

```bash
asyncmq worker deregister worker42
```

* Pipe output through your own `jq` or `grep` to integrate AsyncMQ into shell scripts and dashboards!

With this CLI at your fingertips, you can administer queues, recover from failures, and scale workers,
all without leaving your terminal. Enjoy the power (and the pretty panels)! 🎉
