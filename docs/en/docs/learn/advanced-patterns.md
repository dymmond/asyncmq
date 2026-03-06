# Advanced Patterns

## Idempotent Task Handlers

Retries and re-enqueues are normal in distributed systems. Write handlers so duplicate execution is safe.

Examples:

- upsert instead of insert
- external side effects guarded by idempotency keys
- deterministic output writes

## Dependency Graphs with `FlowProducer`

Use flows when order is part of correctness.

```python
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job

producer = FlowProducer(backend)

extract = Job(task_id="etl.extract", args=[], kwargs={})
transform = Job(task_id="etl.transform", args=[], kwargs={}, depends_on=[extract.id])
load = Job(task_id="etl.load", args=[], kwargs={}, depends_on=[transform.id])

await producer.add_flow("etl", [extract, transform, load])
```

## Worker Lifecycle Hooks

Use hooks for startup/shutdown resources (DB pools, telemetry flush).

```python
class AppSettings(Settings):
    worker_on_startup = [connect_dependencies]
    worker_on_shutdown = [close_dependencies]
```

## Stalled Recovery Loop

If `enable_stalled_check=True`, add a dedicated scheduler task/process for recovery:

```python
from asyncmq.core.stalled import stalled_recovery_scheduler

await stalled_recovery_scheduler()
```

Without this loop, stalled jobs are only tracked, not automatically re-enqueued.
