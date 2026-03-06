# Core Concepts

## Objects You Work With

- `@task`: registers a callable in `TASK_REGISTRY` and adds `.enqueue()/.delay()/.send()`.
- `Job`: serializable unit of work (`task`, `args`, `kwargs`, retries, ttl, status, etc.).
- `Queue`: high-level API bound to a queue name and backend.
- `Worker`/runner functions: consume jobs and execute handlers.
- `BaseBackend`: persistence and coordination contract.

## End-to-End Flow

1. Producer calls `my_task.enqueue(..., backend=...)`.
2. AsyncMQ creates a `Job` and serializes it (`Job.to_dict()`).
3. Backend stores the job as `waiting` (or `delayed`).
4. Worker dequeues, runs `handle_job`, and resolves task function from `TASK_REGISTRY`.
5. Worker updates state/result, retries or DLQ routing, and acknowledges via backend `ack`.

## Job States

`State` values in runtime:

- `waiting`
- `active`
- `completed`
- `failed`
- `delayed`
- `expired`

Typical transitions:

- `waiting -> active -> completed`
- `waiting -> active -> delayed` (retry with backoff)
- `waiting -> active -> failed` (retries exhausted)
- `waiting/active -> expired` (TTL exceeded)

## Delayed and Repeatable Work

- Delayed jobs are scheduled through `enqueue_delayed` and moved back to the main queue by `delayed_job_scanner`.
- In-process repeatables are registered with `Queue.add_repeatable(...)` and scheduled by `repeatable_scheduler` when that queue's worker runs.

## Dependencies and Flows

- Jobs may include `depends_on` IDs.
- Worker checks parent states before execution; unresolved dependencies are re-delayed briefly.
- `FlowProducer.add_flow()` writes dependency graphs, using backend atomic flow support when available.

## Stalled Recovery

If `enable_stalled_check=True`, workers write heartbeats for active jobs.

Recovery loop is provided by `asyncmq.core.stalled.stalled_recovery_scheduler(...)` and must be started by your application if you want automatic re-enqueue of stale active jobs.

## Where to Go Next

- [Tasks](tasks.md)
- [Queues](queues.md)
- [Workers](workers.md)
- [Backends](backends/overview.md)
