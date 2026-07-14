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

Custom producer ids work like BullMQ `jobId`s, but use Pythonic naming:

```python
first = await queue.add(
    "myapp.tasks.rebuild_index",
    kwargs={"scope": "customers"},
    job_id="rebuild-customers",
)

second = await queue.add(
    "myapp.tasks.rebuild_index",
    kwargs={"scope": "customers"},
    job_id="rebuild-customers",
)

assert first == second
```

If a non-removed job with the same `job_id` already exists in the queue, AsyncMQ
returns the existing id and does not enqueue a duplicate record.

The same rule applies to `add_bulk(...)`. Bulk payloads may use either `job_id`
or `jobId` keys, which makes BullMQ migration easier while keeping the Python API
consistent.

Custom ids follow BullMQ-compatible validation rules:

- `job_id` must be a non-empty string.
- numeric-only ids are rejected.
- `:` is rejected because it collides with backend key formats.

BullMQ-style deduplication is also available:

```python
first = await queue.add(
    "myapp.tasks.sync_customer",
    kwargs={"customer_id": "cus_123"},
    deduplication={"id": "customer:cus_123"},
)

second = await queue.add(
    "myapp.tasks.sync_customer",
    kwargs={"customer_id": "cus_123"},
    deduplication={"id": "customer:cus_123"},
)

assert first == second
```

Supported producer modes:

- simple deduplication: `{"id": "..."}`
- throttle deduplication: `{"id": "...", "ttl": 10.0}`
- debounce/replace for delayed jobs:
  `{"id": "...", "ttl": 30.0, "extend": True, "replace": True}`

For full semantics, production advice, and BullMQ comparisons, see
[Deduplication](deduplication.md).

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

## Inspection and Admin APIs

AsyncMQ now exposes a BullMQ-style inspection surface directly from `Queue`:

```python
job = await queue.get_job(job_id)
state = await queue.get_job_state(job_id)
result = await queue.get_job_result(job_id)

counts = await queue.get_job_counts("waiting", "delayed", "failed")
total = await queue.get_job_count_by_types("waiting", "delayed")
queued_work = await queue.count()

page = await queue.get_jobs(["waiting", "delayed"], start=0, end=49, asc=True)
waiting = await queue.get_waiting()
completed = await queue.get_completed()
waiting_children = await queue.get_waiting_children()
```

The inspection model is portable across backends:

- `waiting-children` is inferred from unresolved dependency metadata.
- `paused` and `prioritized` are exposed as inspection buckets for API parity, but AsyncMQ does not persist them as separate backend states.
- queue pause remains queue-level control, and priority remains waiting-queue ordering metadata.

Administrative cleanup helpers are also available:

```python
removed_waiting = await queue.drain()
removed_all_queued = await queue.drain(include_delayed=True)
removed_completed = await queue.clean_jobs(grace=300, limit=100, state="completed")
obliterated = await queue.obliterate(force=True)
removed = await queue.remove_job(job_id)
retried = await queue.retry_job(job_id)
```

Legacy `queue.clean(state, older_than)` and BullMQ-style `clean_jobs(...)`
remove matching jobs through the backend removal path, so queue membership,
broker deliveries, and inspection metadata stay aligned.

Deduplication inspection and release helpers are part of the same producer/admin
surface:

```python
owner = await queue.get_deduplication_job_id("search:products")
owner_legacy = await queue.get_debounce_job_id("search:products")
released = await queue.remove_deduplication_key("search:products")
```

## Delayed and Repeatable APIs

```python
await queue.list_delayed()
await queue.remove_delayed(job_id)
await queue.list_repeatables()
next_run = await queue.upsert_repeatable("myapp.tasks.cleanup", every=300)
await queue.pause_repeatable(job_def)
await queue.resume_repeatable(job_def)
await queue.remove_repeatable(job_def)
```

`Queue.add_repeatable(...)` registers an in-process repeatable used only by the current worker process.

`Queue.upsert_repeatable(...)` persists a schedule in the backend so workers and dashboard flows can discover it without inheriting local producer state. Prefer `upsert_repeatable(...)` for production scheduling and keep `add_repeatable(...)` for tests, local development, or worker bootstrap defined in code.

Repeatables stored in the backend are coordinated under a per-queue scheduler lock
so multiple workers do not all advance the same durable schedule at once.
Redis and Postgres provide distributed coordination here; in-memory and MongoDB
provide process-local coordination.

## Running a Worker from Queue

```python
await queue.run()  # async
queue.start()      # blocking wrapper
```

`queue.run()` calls `run_worker(...)` with queue-level concurrency, rate limit, scan interval, local repeatables, and repeatables stored in the backend.
