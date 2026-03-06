# Queues

`asyncmq.queues.Queue` is the main producer-side API and a convenient worker entrypoint.

```python
from asyncmq.queues import Queue

queue = Queue("default")
```

## Constructor

```python
Queue(
    name: str,
    backend: BaseBackend | None = None,
    concurrency: int = 3,
    rate_limit: int | None = None,
    rate_interval: float = 1.0,
    scan_interval: float | None = None,
)
```

- If `backend` is omitted, `asyncmq.monkay.settings.backend` is used.
- `scan_interval` overrides global settings for delayed/repeatable scanner loops.

## Adding Jobs

High-level helpers:

```python
job_id = await queue.add("myapp.tasks.send_email", args=["a@b.com"], retries=2)
ids = await queue.add_bulk([
    {"task_id": "myapp.tasks.a", "args": [1]},
    {"task_id": "myapp.tasks.b", "kwargs": {"x": 2}},
])
```

Lower-level payload helpers:

```python
await queue.enqueue(payload)
await queue.enqueue_delayed(payload, run_at)
await queue.delay(payload, run_at=None)  # alias helper
await queue.send(payload)                # alias of enqueue
```

## Queue Control

```python
await queue.pause()
await queue.resume()
stats = await queue.queue_stats()
jobs = await queue.list_jobs("waiting")
await queue.clean("completed")
```

## Delayed and Repeatable APIs

```python
await queue.list_delayed()
await queue.remove_delayed(job_id)
await queue.list_repeatables()
await queue.pause_repeatable(job_def)
await queue.resume_repeatable(job_def)
```

`Queue.add_repeatable(...)` registers in-process repeatables used when this queue runs through `queue.run()` / `run_worker(...)`.

## Running a Worker from Queue

```python
await queue.run()  # async
queue.start()      # blocking wrapper
```

`queue.run()` calls `run_worker(...)` with queue-level concurrency, rate-limit, scan interval, and repeatables.
