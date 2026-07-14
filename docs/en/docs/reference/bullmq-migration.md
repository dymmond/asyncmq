# BullMQ Migration Guide

This guide is for teams that already know BullMQ and want to move to AsyncMQ
without losing the operational model they rely on.

The short version:

- most day-to-day concepts map cleanly
- AsyncMQ keeps the model async and natural for Python
- AsyncMQ avoids Redis lock-in by expressing capabilities through backend contracts

## Concept Mapping

| BullMQ concept | AsyncMQ equivalent |
| --- | --- |
| `Queue` | `asyncmq.queues.Queue` |
| `Worker` | `Queue.run()`, `run_worker(...)`, or `Worker.run()` |
| `QueueScheduler` | delayed scanner + repeatable scheduler + optional stalled recovery |
| `FlowProducer` | `asyncmq.flow.FlowProducer` |
| `jobId` | `job_id=` on `Queue.add(...)` |
| deduplication / debounce | `deduplication={...}` or `debounce={...}` |
| repeatable job scheduler | `Queue.upsert_repeatable(...)` |
| queue getters / counts / clean / drain / obliterate | AsyncMQ queue inspection/admin APIs |

## Producer Migration

BullMQ:

```ts
await queue.add("send-email", { email: "alice@example.com" }, { jobId: "welcome-alice" });
```

AsyncMQ:

```python
await queue.add(
    "myapp.tasks.send_email",
    kwargs={"email": "alice@example.com"},
    job_id="welcome-alice",
)
```

The practical semantics are the same:

- if a non-removed job with that id already exists in the queue
- AsyncMQ returns the existing id and does not enqueue a duplicate

## Bulk Producer Migration

BullMQ:

```ts
await queue.addBulk([
  { name: "sync-a", data: { id: 1 }, opts: { jobId: "sync-1" } },
  { name: "sync-b", data: { id: 2 } },
]);
```

AsyncMQ:

```python
await queue.add_bulk(
    [
        {"task_id": "sync.a", "kwargs": {"id": 1}, "jobId": "sync-1"},
        {"task_id": "sync.b", "kwargs": {"id": 2}},
    ]
)
```

AsyncMQ accepts both `jobId` and `job_id` in bulk payloads to make migration
easier.

## Deduplication Migration

BullMQ:

```ts
await queue.add("rebuild", {}, {
  deduplication: { id: "search:products", ttl: 3000 }
});
```

AsyncMQ:

```python
await queue.add(
    "search.rebuild",
    deduplication={"id": "search:products", "ttl": 3.0},
)
```

Notes:

- AsyncMQ uses seconds for timing values in Python APIs
- deduplication is stored on job metadata, not Redis specific side keys
- the practical behavior is still simple deduplication, throttle windows, and debounce replacement

## Worker Migration

BullMQ usually looks like this:

```ts
const worker = new Worker("emails", async job => {
  ...
}, { concurrency: 8 });
```

In AsyncMQ the handler is defined separately and workers consume by queue:

```python
from asyncmq.tasks import task


@task(queue="emails", retries=3)
async def send_email(email: str) -> None:
    ...
```

```python
from asyncmq.queues import Queue

queue = Queue("emails", concurrency=8)
await queue.run()
```

This separation is the biggest ergonomic difference:

- BullMQ often passes the processor inline to the worker
- AsyncMQ resolves the handler through the Python task registry

## Scheduler Migration

BullMQ users often think in terms of `QueueScheduler`. AsyncMQ splits that role
into explicit loops:

- delayed job scanner
- repeatable scheduler
- optional stalled recovery scheduler

For most users, `Queue.run()` is enough because it starts delayed and
repeatable scheduling automatically.

If you enable stalled recovery, `Queue.run()` starts recovery too. If you build
a custom lower-level worker loop, also run:

```python
from asyncmq.core.stalled import stalled_recovery_scheduler

await stalled_recovery_scheduler()
```

## Repeatable Job Migration

BullMQ repeatables map to durable AsyncMQ repeatables:

```python
await queue.upsert_repeatable(
    "reports.generate_daily",
    cron="0 6 * * *",
    kwargs={"tenant": "acme"},
)
```

AsyncMQ also has a helper for local worker setup:

```python
queue.add_repeatable("maintenance.cleanup", every=300)
```

Use `add_repeatable(...)` only when the worker process itself is the intended
source of truth for that schedule.

## Flow Migration

BullMQ's `FlowProducer` maps directly in spirit:

```python
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job

producer = FlowProducer(backend)

parent = Job(task_id="etl.extract", args=[], kwargs={})
child = Job(task_id="etl.transform", args=[], kwargs={}, depends_on=[parent.id])

await producer.add_flow("etl", [parent, child])
```

Important behavioral note:

- AsyncMQ supports dependency gating and `waiting-children` inspection
- AsyncMQ does not try to clone every BullMQ failure policy API for flows

If your BullMQ usage depends on advanced parent state policies, model those
policies explicitly in application logic or operator workflows.

## Queue Inspection Migration

Common mappings:

```python
await queue.get_job(job_id)
await queue.get_jobs(["waiting", "delayed"], start=0, end=49)
await queue.get_job_counts("waiting", "active", "completed", "failed")
await queue.clean_jobs(grace=3600, limit=1000, state="completed")
await queue.drain(include_delayed=True)
await queue.obliterate(force=True)
```

Intentional difference:

- `paused` and `prioritized` are inspection buckets for parity, not separate
  persisted backend buckets

## Events and Telemetry

BullMQ users may expect `QueueEvents` based on Redis streams. AsyncMQ takes a
different route:

- local lifecycle events through the event emitter
- dashboard telemetry and SSE surfaces
- backend broadcast hooks where supported

This is one of the largest intentional differences because AsyncMQ is not built
around Redis streams as the universal event substrate.

## Operational Differences to Expect

### Naming

- BullMQ: JavaScript surface that leans on camelCase
- AsyncMQ: Pythonic snake_case surface

### Timing units

- BullMQ commonly uses milliseconds
- AsyncMQ Python APIs use seconds

### Backend model

- BullMQ is built around Redis
- AsyncMQ is portable across backends and lets coordination quality vary by backend

### Queue scheduler model

- BullMQ historically centralized more behavior in Redis scheduler concepts
- AsyncMQ exposes explicit runtime loops and backend capability contracts

## Migration Checklist

1. replace inline BullMQ processors with `@task(queue=...)` handlers
2. translate `jobId` to `job_id`
3. convert timing values from milliseconds to seconds
4. migrate repeatables to `upsert_repeatable(...)`
5. review any flow logic that depends on advanced parent-failure policies
6. choose the AsyncMQ backend that matches your coordination requirements
7. wire delayed, repeatable, and stalled recovery loops appropriately
8. validate queue inspection and dashboard workflows in staging

## Related

- [BullMQ Parity](bullmq-parity.md)
- [Backend Capabilities](backend-capabilities.md)
- [Queues](../features/queues.md)
- [Workers](../features/workers.md)
- [Schedulers](../features/schedulers.md)
