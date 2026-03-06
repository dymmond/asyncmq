# Runners

`asyncmq.runners` contains worker entrypoints.

## `worker_loop(queue_name, backend=None)`

Basic loop:

- dequeue
- `handle_job`
- sleep

Useful for simple/testing scenarios.

## `start_worker(queue_name, concurrency=1)`

Creates a `Worker` object and runs it.

Used by CLI command:

```bash
asyncmq worker start <queue> --concurrency <n>
```

## `run_worker(...)`

Feature-rich runner used by `Queue.run()`:

```python
await run_worker(
    queue_name,
    backend=None,
    concurrency=3,
    rate_limit=None,
    rate_interval=1.0,
    repeatables=None,
    scan_interval=None,
)
```

It wires together:

- `process_job(...)`
- `delayed_job_scanner(...)`
- `repeatable_scheduler(...)` (only if repeatables are provided)
- worker registration/deregistration
- startup/shutdown hooks

## Choosing an Entry Point

- Use `asyncmq worker start` for CLI operation.
- Use `Queue.run()` / `Queue.start()` for app-owned workers.
- Use `run_worker(...)` directly when you need explicit control.
