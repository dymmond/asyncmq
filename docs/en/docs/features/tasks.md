# Tasks

Tasks are registered with `asyncmq.tasks.task`.

```python
from asyncmq.tasks import task


@task(queue="emails", retries=3, ttl=300, progress=False)
async def send_email(address: str) -> None:
    ...
```

## What the Decorator Adds

When you decorate a function, AsyncMQ:

- registers metadata in `TASK_REGISTRY` under `module.function`
- wraps the callable in `TaskWrapper`
- adds helper methods:
- `.enqueue(*args, backend=..., delay=0, priority=5, depends_on=None, repeat_every=None, **kwargs)`
- `.delay(...)` alias
- `.send(...)` alias

## Calling vs Enqueuing

- Calling `send_email("a@b.com")` executes immediately.
- Calling `await send_email.enqueue("a@b.com", backend=queue.backend)` creates a job.

## Return Value of `.enqueue()`

- Immediate enqueue: returns `job_id` (`str`).
- Delayed enqueue (`delay > 0`): schedules with `enqueue_delayed(...)` and returns `None`.

## Progress Events

Set `progress=True` to get `report_progress` injected into your task signature.

```python
@task(queue="imports", progress=True)
async def import_data(path: str, report_progress):
    report_progress(0.5, {"step": "parsed"})
```

Progress emits `job:progress` events through `event_emitter`.

## Dependency-Aware Enqueue

Use `depends_on=[parent_job_id, ...]` to block execution until parent jobs are completed.

```python
child_id = await task_b.enqueue(backend=backend, depends_on=[parent_id])
```

## Error and Retry Behavior

Retries are controlled by task/job metadata (`retries`, `backoff`).
Actual retry handling happens in worker runtime (`handle_job`).

Next: [Jobs](jobs.md) and [Workers](workers.md).
