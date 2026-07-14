# Schedulers

AsyncMQ scheduling is split into explicit loops instead of one Redis specific
scheduler abstraction. That keeps the runtime portable across backends while still
covering the practical BullMQ feature set:

- delayed jobs
- repeatable jobs
- cron schedules
- interval schedules
- durable schedule storage
- schedule pause/resume/remove operations
- multi-worker scheduler ownership

## Scheduler Components

AsyncMQ has three important timing-related loops:

### Delayed job scanner

`asyncmq.core.delayed_scanner.delayed_job_scanner(...)`

Moves jobs whose `delay_until` timestamp is due back into the main queue.

### Repeatable scheduler

`asyncmq.schedulers.repeatable_scheduler(...)`

Generates concrete job instances from repeatable definitions.

### Stalled recovery scheduler

`asyncmq.core.stalled.stalled_recovery_scheduler(...)`

Detects active jobs that appear abandoned and re-enqueues them.

This page focuses on delayed and repeatable scheduling. See
[Workers](workers.md) for stalled recovery.

## Delayed Jobs

Delayed jobs are regular jobs with a future `delay_until` timestamp.

You can create them directly:

```python
await queue.add(
    "notifications.send_digest",
    kwargs={"tenant": "acme"},
    delay=300.0,
)
```

Or they can be created indirectly by:

- retry backoff after handler failures
- dependency re-delay while waiting for parents
- debounce replacement workflows

The delayed scanner:

1. asks the backend to promote due delayed entries
2. lets the backend move them back into the normal queue through its own
   lifecycle transition
3. sleeps for the configured interval

Transient backend promotion errors are logged and retried on the next scan
instead of stopping the scanner loop.

`Queue.run()` and `run_worker(...)` start this scanner automatically.

## Repeatable Jobs

Repeatable jobs generate a new concrete job each time their schedule fires.

AsyncMQ supports two registration models.

### Local repeatables

```python
queue.add_repeatable("maintenance.compact_indexes", every=300)
```

Use this when:

- the schedule is defined in code
- one worker process is intentionally the source of truth
- you are running tests or local development loops

Local repeatables live only in the current process.

### Durable repeatables

```python
await queue.upsert_repeatable(
    "billing.send_statement",
    cron="0 7 * * 1",
    kwargs={"region": "eu"},
    retries=3,
)
```

Use this when:

- schedules must survive producer or worker restarts
- operators create schedules through admin tooling or the dashboard
- multiple worker processes may consume the same queue

Durable repeatables are stored by the backend and discovered by workers.

## `every` vs `cron`

Use `every` for simple fixed intervals:

```python
await queue.upsert_repeatable("cleanup.temp_files", every=600)
```

Use `cron` for wall-clock schedules:

```python
await queue.upsert_repeatable("reports.daily", cron="0 6 * * *")
```

Practical rule:

- `every` is easier when exact human calendar alignment does not matter
- `cron` is better when the run time should line up with business hours,
  calendar boundaries, or operator expectations

## How Repeatable Scheduling Works

The repeatable scheduler does two separate jobs:

1. process local in-memory repeatable definitions passed in through `Queue`
2. poll durable repeatables stored in the backend and advance the next-run marker

When a schedule is due, the scheduler:

- builds one concrete `Job`
- enqueues it into the target queue
- computes the next scheduled execution time
- stores that next run for durable schedules

Generated job instances are normal jobs. They still go through the standard
worker lifecycle, retries, TTL, deduplication, events, and admin inspection.

Transient enqueue or backend repeatable-store errors are logged and retried by
later scheduler iterations. Local repeatable definitions advance their in-memory
last-run marker only after a generated job is successfully enqueued.

## Scheduler Ownership

Multiple workers for the same queue may all start a repeatable scheduler. To
avoid duplicate enqueue of durable schedules, AsyncMQ processes
repeatables stored in the backend under a per-queue scheduler lock.

What this means:

- Redis and Postgres coordinate scheduler ownership across processes
- in-memory and MongoDB coordinate only inside one process
- RabbitMQ inherits the coordination quality of its metadata store

This is the AsyncMQ equivalent of BullMQ's need for centralized schedule
advancement, but expressed through backend capability contracts instead of a
Redis specific `QueueScheduler`.

## Schedule Management APIs

The queue API exposes a complete operational surface:

```python
records = await queue.list_repeatables()

next_run = await queue.upsert_repeatable(
    "reporting.rebuild",
    every=900,
    kwargs={"tenant": "acme"},
)

await queue.pause_repeatable({"task_id": "reporting.rebuild", "every": 900, "kwargs": {"tenant": "acme"}})
await queue.resume_repeatable({"task_id": "reporting.rebuild", "every": 900, "kwargs": {"tenant": "acme"}})
await queue.remove_repeatable({"task_id": "reporting.rebuild", "every": 900, "kwargs": {"tenant": "acme"}})
```

Operationally:

- `upsert_repeatable(...)` creates or updates the schedule definition
- `list_repeatables()` exposes next run time and paused status
- `pause_repeatable(...)` stops future occurrences without deleting the schedule
- `resume_repeatable(...)` recomputes the next run
- `remove_repeatable(...)` deletes the durable definition

The dashboard uses the same backend-facing model, so these APIs are suitable
for future admin tooling as well.

## Production Example

Code-defined bootstrap schedule plus durable tenant schedules:

```python
import anyio

from asyncmq.queues import Queue
from myapp import tasks  # noqa: F401


async def main() -> None:
    queue = Queue("maintenance", concurrency=4, scan_interval=2.0)

    # Local, code-owned housekeeping schedule.
    queue.add_repeatable("maintenance.cleanup_tmp", every=300)

    # Durable schedule shown to operators.
    await queue.upsert_repeatable(
        "maintenance.rotate_keys",
        cron="0 2 * * 0",
        kwargs={"scope": "prod"},
        retries=5,
    )

    await queue.run()


anyio.run(main)
```

## Failure and Recovery Behavior

Delayed and repeatable scheduling are restart-safe only to the extent the
backend persists the underlying metadata.

Practical expectations:

- Redis and Postgres promote due delayed jobs through atomic backend
  transitions
- MongoDB and in-memory promote due delayed jobs under their process-local
  backend locks
- MongoDB preserves schedules but uses process-local coordination
- in-memory loses everything on process exit
- RabbitMQ durability depends on the broker plus the chosen metadata store;
  delayed promotion is backend-owned, but broker publish and metadata update
  are not a single distributed transaction

If a worker restarts:

- delayed jobs remain delayed in durable backends until scanned again
- durable repeatables are rediscovered from backend storage
- local repeatables must be re-registered by application code

## Tuning

Use `scan_interval` to control how often scheduling loops poll:

```python
queue = Queue("emails", scan_interval=1.0)
```

Lower values:

- reduce delay between a due timestamp and actual enqueue
- increase backend polling and operational noise

Higher values:

- reduce backend churn
- increase scheduling latency

For most workloads, a small single-digit number of seconds is a reasonable
starting point.

## Debugging Checklist

If delayed or repeatable jobs are not appearing:

1. confirm the worker was started with `Queue.run()` or `run_worker(...)`
2. check the queue's `scan_interval`
3. confirm the task module is imported
4. list durable schedules with `queue.list_repeatables()`
5. check backend capabilities for coordination guarantees
6. verify the queue is not paused

Useful inspection commands:

```python
await queue.get_delayed()
await queue.list_repeatables()
await queue.get_job_counts("waiting", "delayed", "completed", "failed")
```

## BullMQ Mapping

| BullMQ | AsyncMQ |
| --- | --- |
| delayed jobs | delayed scanner plus `delay_until` job metadata |
| repeatable jobs / job schedulers | `Queue.upsert_repeatable(...)` |
| local worker-owned schedules | `Queue.add_repeatable(...)` |
| remove repeatable | `Queue.remove_repeatable(...)` |
| pause/resume repeatable | `Queue.pause_repeatable(...)` / `Queue.resume_repeatable(...)` |

AsyncMQ intentionally separates schedule generation from worker execution loops
instead of depending on Redis scheduler scripts.

## Common Mistakes

- Using `add_repeatable(...)` for production schedules that must survive restart.
- Forgetting that `scan_interval` affects delayed and repeatable latency.
- Running multiple workers on a backend with only process-local locking and
  expecting cluster-wide schedule ownership.
- Treating repeatable jobs as a separate job type rather than a generator of
  normal jobs.

## Related

- [Queues](queues.md)
- [Workers](workers.md)
- [Flows](flows.md)
- [Backend Capabilities](../reference/backend-capabilities.md)
