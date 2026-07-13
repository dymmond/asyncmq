# Jobs

`asyncmq.jobs.Job` is AsyncMQ's durable unit of work. Producers create jobs,
backends persist them, workers execute them, and admin APIs inspect them.

If you understand the `Job` payload, you understand most of AsyncMQ's public
runtime contract.

## What a Job Contains

```python
Job(
    task_id: str,
    args: list[Any],
    kwargs: dict[str, Any],
    retries: int = 0,
    max_retries: int = 3,
    backoff: float | int | Callable | None = 1.5,
    ttl: int | None = None,
    job_id: str | None = None,
    created_at: float | None = None,
    priority: int = 5,
    repeat_every: float | int | None = None,
    depends_on: list[str] | None = None,
    deduplication: dict[str, Any] | None = None,
)
```

The most important fields are:

- `task_id`: the registry key that resolves to the actual handler function.
- `args` and `kwargs`: the serialized call payload.
- `job_id`: the stable identifier stored as `id` in the payload.
- `retries` and `max_retries`: attempt counters used by the worker.
- `backoff`: how the next retry delay is computed after a failure.
- `ttl`: staleness limit measured from `created_at`.
- `priority`: waiting-queue ordering metadata.
- `delay_until`: runtime scheduling field used for delayed jobs and retries.
- `depends_on`: parent job ids that must complete before this job can run.
- `deduplication`: producer-side metadata for BullMQ-style deduplication,
  debounce, and throttle behavior.

## Lifecycle Semantics

The default status of a newly created job is `waiting`.

Typical transitions are:

- `waiting -> active -> completed`
- `waiting -> active -> delayed -> active -> completed`
- `waiting -> active -> failed`
- `waiting -> delayed -> waiting -> active`
- `waiting-children -> waiting -> active`
- `waiting/active -> expired`

AsyncMQ does not materialize every inspection bucket as a separate persisted
state. In particular:

- `waiting-children` is derived from unresolved `depends_on` metadata.
- `paused` is queue-level control, not a job-level persisted state.
- `prioritized` is ordering metadata on waiting jobs, not a separate job bucket.

That design keeps the model portable across Redis, Postgres, MongoDB,
RabbitMQ metadata stores, and in-memory backends.

## Retries and Backoff

When a handler raises an exception:

1. `last_error` and `error_traceback` are stored on the job payload
2. `retries` is incremented
3. the worker computes `next_retry_delay()`
4. the job is re-enqueued into the delayed queue if attempts remain
5. otherwise the job is marked `failed` and moved to the DLQ

Manual retry APIs move failed jobs back to `waiting` and clear terminal result
and error fields before the next execution attempt.

Backoff supports several modes:

- numeric exponential base: `1.5`, `2`, `3`
- callable taking the current retry count
- callable taking no arguments
- `None` for immediate retry

Example:

```python
from asyncmq.tasks import task


def bounded_backoff(attempt: int) -> float:
    return min(60.0, 2.0**attempt)


@task(queue="billing", retries=5, backoff=bounded_backoff)
async def charge_invoice(invoice_id: str) -> None:
    ...
```

## TTL and Expiration

`ttl` is measured from `created_at`, not from the moment a worker first sees
the job.

That means TTL protects you from stale work such as:

- confirmation emails that no longer matter
- rebuild jobs superseded by newer requests
- time-sensitive downstream calls

If a job has already expired when the worker evaluates it:

- the status becomes `expired`
- the job is acknowledged
- `job:expired` is emitted
- the payload is moved to the DLQ

Use TTL when executing stale work is worse than dropping it.

## Delays, Priority, and Scheduling Metadata

AsyncMQ stores delayed execution directly on the job payload through
`delay_until`.

This field is used for:

- producer-created delayed jobs
- retry backoff
- debounce replacement windows

Priority is separate from delay:

- delay controls *when* a job becomes eligible
- priority controls *which eligible waiting job* should be consumed first

BullMQ users should think of this as the same practical model, expressed as a
backend-neutral Python payload instead of Redis-only state structures.

## Dependencies and `waiting-children`

Jobs may declare `depends_on=[parent_job_id, ...]`.

Workers will not execute the child until all parents have completed. While
dependencies remain unresolved:

- the job appears in `waiting-children` inspection views
- runtime gating keeps it from executing
- completion of a parent removes that parent id from the child payload

Example:

```python
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job

extract = Job(task_id="etl.extract", args=[], kwargs={})
transform = Job(task_id="etl.transform", args=[], kwargs={}, depends_on=[extract.id])
load = Job(task_id="etl.load", args=[], kwargs={}, depends_on=[transform.id])
```

Important operational detail:

- children are released by parent completion
- a permanently failed parent does not automatically fail descendants
- blocked descendants remain inspectable until you retry, remove, or repair the parent

That is simpler than BullMQ's richer flow-specific failure policies, but it is
explicit and backend-neutral.

## `job_id` and Deduplication

`job_id` and `deduplication` solve different problems:

- `job_id` prevents a duplicate record for one exact queue-scoped identity
- `deduplication` prevents logically equivalent work from being created again

Example:

```python
job_id = await queue.add(
    "search.rebuild",
    kwargs={"scope": "products"},
    job_id="rebuild-products",
    deduplication={"id": "search:products", "ttl": 30.0},
)
```

Use `job_id` when an operator or API needs to refer to one specific job.
Use deduplication when several producers may request the same logical work.

See [Deduplication](deduplication.md) for mode-specific behavior.

## Serialization Contract

`Job.to_dict()` and `Job.from_dict()` define the payload shape that custom
backends, admin tools, and external integrations should expect.

Important keys:

- `id`: stable job identifier
- `task`: task registry id
- `args` and `kwargs`: serialized call arguments
- `status`: current runtime state
- `delay_until`: next eligible execution time
- `created_at`: creation timestamp
- `result`: stored completion result if available
- `last_error` and `error_traceback`: failure metadata
- `depends_on`: unresolved parent ids
- `deduplication`: active deduplication metadata when present

If you implement a custom backend, preserve unknown keys rather than filtering
them out. That keeps forward compatibility with newer runtime metadata.

## Production Advice

- Keep payloads JSON-serializable and stable across deploys.
- Keep job arguments small; store large blobs externally and pass references.
- Make handlers idempotent because retries, retries after restarts, and manual
  replays are normal.
- Use explicit `max_retries` and bounded backoff for external systems.
- Use TTL for work that becomes harmful when stale.
- Use queue inspection APIs to clean completed and failed jobs instead of
  relying on backend-specific manual deletes.

## Common Mistakes

- Treating `ttl` as a worker timeout. It is a staleness window, not a hard
  execution deadline.
- Using huge payloads that make backends and dashboards expensive to operate.
- Depending on `waiting-children` jobs to fail automatically when a parent
  fails.
- Forgetting that `retries` is attempt history, not remaining attempts.

## Related

- [Queues](queues.md)
- [Workers](workers.md)
- [Schedulers](schedulers.md)
- [Flows](flows.md)
