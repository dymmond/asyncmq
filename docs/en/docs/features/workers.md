# Workers

Workers are the runtime side of AsyncMQ. They pull jobs from a queue, enforce
state transitions, execute handlers, apply retries and backoff, and publish
lifecycle events.

## Which Worker API to Use

AsyncMQ exposes three practical entrypoints:

### `Queue.run()` / `Queue.start()`

Use this for the full queue runtime:

- job consumption
- delayed job scanning
- repeatable scheduling
- queue-level concurrency and rate limiting

This is the most common production entrypoint for application-owned workers.

### `run_worker(...)`

This is the functional equivalent of `Queue.run()` and is useful when you want
to wire the runtime explicitly from your own bootstrap code.

### `Worker`

`asyncmq.workers.Worker` is a lighter wrapper around `process_job(...)` plus
worker registration and lifecycle hooks. It is useful when you want the lower
level process loop directly.

Important distinction:

- `Queue.run()` and `run_worker(...)` also start delayed and repeatable schedulers
- `Worker.run()` does not start those scheduler loops for you

## What a Worker Actually Does

The core loop lives in `process_job(...)` and `handle_job(...)`.

For each queue, the runtime repeatedly:

1. checks whether the queue is paused
2. dequeues the next eligible job
3. applies concurrency limits
4. applies optional per-process rate limiting
5. normalizes backend payload shape
6. evaluates dependency gating, cancellation, TTL, and delay rules
7. moves the job to `active`
8. executes the task directly or in the sandbox
9. records completion, retry, or terminal failure

That means the worker owns the operational truth of job lifecycle behavior even
though storage details vary by backend.

## Execution Lifecycle

The job execution path is:

1. normalize the backend payload into a `Job`
2. check `depends_on`; unresolved parents keep the job blocked
3. check cancellation
4. check TTL expiration
5. check `delay_until`
6. move to `active` and emit `job:started`
7. optionally record a job heartbeat for stalled detection
8. execute the handler
9. on success:
   - mark `completed`
   - persist the result
   - acknowledge the job
   - resolve dependencies for children
   - emit `job:completed`
10. on failure:
   - capture traceback metadata
   - increment retries
   - either requeue with backoff as `delayed`
   - or mark `failed`, acknowledge, move to the DLQ, and emit `job:failed`

This is broadly the same mental model as BullMQ workers, expressed through
backend-neutral Python runtime code instead of Redis scripts.

## Concurrency

Concurrency is enforced per worker process through an `anyio.CapacityLimiter`.

Example:

```python
from asyncmq.queues import Queue

queue = Queue("emails", concurrency=8)
await queue.run()
```

What that means in practice:

- one worker process with `concurrency=8` can run up to eight jobs at once
- four worker processes with `concurrency=8` can run up to thirty-two jobs at once
- concurrency is local to that worker process, not a global cluster-wide number

Choose concurrency based on the handler's bottleneck:

- I/O-heavy jobs usually tolerate higher concurrency
- CPU-heavy jobs usually need lower concurrency or separate worker pools

## Rate Limiting

`rate_limit` and `rate_interval` apply a token-bucket limiter inside the
worker runtime.

```python
queue = Queue("outbound-api", concurrency=20, rate_limit=10, rate_interval=1.0)
```

That example means:

- up to twenty jobs may be in flight locally
- but no more than ten jobs per second will start in that worker process

Important limitation:

- the built-in limiter is per worker process
- it is not a distributed global rate limit shared across all workers

For global downstream protection, combine AsyncMQ worker settings with
partitioning, external quotas, or one dedicated worker pool for the constrained
integration.

## Pause and Resume

Queue pause is checked before dequeueing new work.

When a queue is paused:

- workers stop claiming new jobs
- active jobs may continue to finish
- delayed and repeatable schedulers may still maintain queue metadata, but no
  new dequeues occur until resume

```python
await queue.pause()
await queue.resume()
```

Backend note:

- pause guarantees depend on the backend implementation
- Redis provides durable shared pause state
- some backends intentionally keep pause metadata process-local

Check [Backend Capabilities](../reference/backend-capabilities.md) when
multi-process operational control matters.

## Worker Registration and Heartbeats

AsyncMQ tracks two related but different signals:

### Worker heartbeats

Workers register themselves with the backend so dashboard and admin surfaces
can show:

- worker id
- queue assignment
- concurrency
- last heartbeat timestamp

### Job heartbeats

If `enable_stalled_check=True`, the worker records a heartbeat for the job
when execution starts.

Long-running jobs that may exceed `stalled_threshold` should refresh that
heartbeat explicitly:

```python
from asyncmq.core.stalled import record_heartbeat
from asyncmq.tasks import task


@task(queue="video")
async def transcode(job_id: str) -> None:
    for chunk in range(10):
        ...
        await record_heartbeat("video", job_id)
```

If you do not refresh heartbeats for very long jobs, the stalled recovery loop
may treat them as abandoned work.

## Stalled Recovery

Setting `enable_stalled_check=True` is only half of the feature.

You must also run the recovery loop:

```python
from asyncmq.core.stalled import stalled_recovery_scheduler

await stalled_recovery_scheduler()
```

The recovery scheduler:

- scans for active jobs whose heartbeat is older than `stalled_threshold`
- re-enqueues them
- emits `job:stalled`

This is equivalent in purpose to BullMQ's stalled job handling, but AsyncMQ
keeps the mechanism explicit so you can decide whether to run it in-process or
as a dedicated operational component.

## Sandbox Execution

If `settings.sandbox_enabled=True`, handlers are executed through
`asyncmq.sandbox.run_handler` in a worker thread instead of being awaited
directly in the main runtime path.

Use sandboxing when:

- you need execution isolation for untrusted or fragile handlers
- you want stricter failure boundaries around task execution

Avoid enabling it blindly for every queue if latency is more important than
isolation.

See [Sandbox](sandbox.md) for the execution model and limits.

## Lifecycle Hooks

Startup and shutdown hooks let you bind worker-local resources:

```python
from asyncmq.conf.global_settings import Settings


async def connect_metrics(**kwargs) -> None:
    ...


async def flush_metrics(**kwargs) -> None:
    ...


class AppSettings(Settings):
    worker_on_startup = [connect_metrics]
    worker_on_shutdown = [flush_metrics]
```

Hooks receive keyword arguments such as:

- `backend`
- `worker_id`
- `queue`

Use hooks for:

- opening and closing client pools
- warming caches
- connecting telemetry sinks
- draining buffers on shutdown

Do not use hooks for heavyweight application migrations or one-time setup.

## Graceful Shutdown

AsyncMQ uses cooperative cancellation:

- `queue.start()` blocks until interrupted
- `await queue.run()` runs until cancelled
- `Worker.stop()` cancels a running `Worker.start()` loop

On shutdown, AsyncMQ attempts to:

- deregister the worker
- run `worker_on_shutdown` hooks safely

Operational recommendation:

- stop claiming new work first
- allow a drain window for in-flight jobs when possible
- keep handlers idempotent so interrupted work can be retried safely

## Production Example

A common production topology is:

1. dedicated producer application processes
2. one or more worker deployments per queue class
3. one stalled-recovery process if stalled recovery is enabled
4. dashboard or metrics readers as separate operational services

Example bootstrap:

```python
import anyio

from asyncmq.core.stalled import stalled_recovery_scheduler
from asyncmq.queues import Queue
from myapp import tasks  # noqa: F401


async def main() -> None:
    emails = Queue("emails", concurrency=16, rate_limit=40, rate_interval=1.0)
    webhooks = Queue("webhooks", concurrency=8)

    async with anyio.create_task_group() as tg:
        tg.start_soon(emails.run)
        tg.start_soon(webhooks.run)
        tg.start_soon(stalled_recovery_scheduler)


anyio.run(main)
```

For larger deployments, separate these into independent OS processes or
containers rather than one big combined process.

## BullMQ Mapping

| BullMQ | AsyncMQ |
| --- | --- |
| `new Worker(queue, processor, ...)` | `Queue.run()`, `run_worker(...)`, or `Worker.run()` |
| worker concurrency | `Queue(..., concurrency=...)` |
| worker rate limiter | `Queue(..., rate_limit=..., rate_interval=...)` |
| queue pause/resume | `await queue.pause()` / `await queue.resume()` |
| stalled detection | `enable_stalled_check=True` plus `stalled_recovery_scheduler(...)` |

BullMQ's old `QueueScheduler` role is split in AsyncMQ between the delayed
scanner, repeatable scheduler, and optional stalled recovery loop.

## Common Mistakes

- Running `Worker.run()` and expecting delayed or repeatable jobs to be driven automatically.
- Enabling stalled checks without running `stalled_recovery_scheduler(...)`.
- Treating `rate_limit` as a cluster-wide quota instead of a per-process limiter.
- Setting concurrency high for CPU-bound handlers and then blaming the backend.
- Forgetting to import task modules before bootstrapping a worker runtime.

## Related

- [Queues](queues.md)
- [Jobs](jobs.md)
- [Schedulers](schedulers.md)
- [Runners](runners.md)
- [Troubleshooting](../troubleshooting.md)
