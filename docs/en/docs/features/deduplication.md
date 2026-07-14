# Deduplication

BullMQ supports more than custom `jobId` uniqueness. It also supports
deduplication windows that ignore or replace jobs while a logical key is
already active.

AsyncMQ now exposes the same practical producer model without depending on
Redis specific key scripts.

## What It Solves

Use deduplication when you want to prevent logically identical work from being
queued repeatedly even if each enqueue request would otherwise receive a new
job id.

Typical cases:

- avoid re-running the same long-lived synchronization
- collapse rapid repeated webhook bursts
- keep only the latest delayed rebuild request
- protect expensive maintenance jobs from duplicate producers

## Modes

AsyncMQ accepts BullMQ-style `deduplication={...}` options and the older
`debounce={...}` alias.

### Simple Mode

No TTL. The deduplication key stays active while the owning job is not in a
terminal state.

```python
job_id = await queue.add(
    "myapp.tasks.sync_customer",
    kwargs={"customer_id": "cus_123"},
    deduplication={"id": "customer:cus_123"},
)
```

If the same call is made again before the job completes or fails, AsyncMQ
returns the original job id and does not enqueue a duplicate.

### Throttle Mode

Set `ttl` to keep the deduplication key active for a time window even if the
job finishes quickly.

```python
job_id = await queue.add(
    "myapp.tasks.refresh_dashboard",
    deduplication={"id": "dashboard:global", "ttl": 10.0},
)
```

For the next ten seconds, later producer calls with the same deduplication id
will return the original job id instead of creating a new job.

### Debounce / Replace Mode

Set `replace=True` on a delayed job to keep only the newest delayed copy.

```python
job_id = await queue.add(
    "myapp.tasks.rebuild_search",
    kwargs={"scope": "products", "version": 42},
    delay=30.0,
    deduplication={
        "id": "search:products",
        "ttl": 30.0,
        "extend": True,
        "replace": True,
    },
)
```

If another delayed enqueue arrives during that window:

- the older delayed job is removed
- the new delayed job becomes the deduplication owner
- the deduplication TTL is refreshed when `extend=True`

AsyncMQ only replaces a job while the current owner is still delayed. If the
owner has already started processing, later calls are deduplicated instead of
replacing in-flight work.

## `job_id` vs Deduplication

These solve different problems:

- `job_id="rebuild-customers"` prevents duplicates for one concrete job id
- `deduplication={"id": "customers:rebuild"}` prevents duplicates for a logical work key

`job_id` uniqueness is scoped to a queue and lasts until the original job is
removed. Deduplication can be released by completion, failure, TTL expiry, or
explicit API calls.

## Queue APIs

Inspect the current deduplication owner:

```python
owner_job_id = await queue.get_deduplication_job_id("search:products")
```

The old BullMQ `debounce` getter name is also available:

```python
owner_job_id = await queue.get_debounce_job_id("search:products")
```

Release a deduplication key without deleting the job itself:

```python
released = await queue.remove_deduplication_key("search:products")
```

This clears the stored deduplication metadata from the owning job payload so
future producer calls can enqueue a fresh copy.

## Bulk Producers

`Queue.add_bulk(...)` routes each entry through the same enqueue rules as
`Queue.add(...)`.

That means bulk producers now preserve:

- custom `job_id` conflict behavior
- deduplication ignore behavior
- debounce/replace rules for delayed jobs

This is slightly less backend-specialized than BullMQ's Redis scripts, but it
keeps behavior consistent across AsyncMQ backends.

## Events

AsyncMQ emits producer-side lifecycle events for deduplication decisions:

- `job:duplicated` for custom `job_id` conflicts
- `job:deduplicated` when a logical deduplication key suppresses a new job
- `job:debounced` as the compatibility alias for BullMQ-style debounce events

These events include queue and job identifiers so dashboards or audit hooks can
track why a producer call did not create a new job.

## Backend Notes

AsyncMQ stores deduplication metadata on the job payload itself rather than in
Redis specific side keys. That keeps the feature portable across:

- Redis
- Postgres
- MongoDB
- RabbitMQ metadata stores
- in-memory test backends

Locking is backend-aware:

- Redis and Postgres coordinate deduplication with distributed locks
- In-memory and MongoDB coordinate within the current process
- RabbitMQ inherits the lock behavior of its metadata store and otherwise falls
  back to process-local coordination

## Failure Behavior

- simple mode releases on terminal state transitions because the job is no
  longer considered an active owner
- TTL-backed modes stay active until the TTL expires, even if the job already
  completed
- manual removal of the owning job disables deduplication immediately because
  the owner record is gone
- replace mode only evicts delayed jobs; it will not remove active work

## Production Advice

- Use `job_id` for strict idempotency of one exact job identity.
- Use deduplication ids for logical workloads that may carry different payloads.
- For debounce flows, keep `delay` and `ttl` aligned so the replace window is
  easy to reason about.
- Prefer Redis or Postgres when multi-process producer coordination matters.
- Do not rely on deduplication as a substitute for task-level idempotency when
  side effects are critical.

## Common Mistakes

- Using `replace=True` without a delayed job and expecting in-flight work to be replaced.
- Reusing a deduplication id for unrelated business operations.
- Forgetting that TTL-backed deduplication can remain active after job completion.
- Treating `job_id` and deduplication ids as interchangeable.

## BullMQ Mapping

| BullMQ | AsyncMQ |
| --- | --- |
| `deduplication: { id }` | `deduplication={"id": ...}` |
| `deduplication: { id, ttl }` | `deduplication={"id": ..., "ttl": ...}` |
| debounce alias | `debounce={...}` or `deduplication={...}` |
| `getDeduplicationJobId(...)` | `queue.get_deduplication_job_id(...)` |
| `getDebounceJobId(...)` | `queue.get_debounce_job_id(...)` |
| `removeDeduplicationKey(...)` | `queue.remove_deduplication_key(...)` |

AsyncMQ keeps BullMQ's practical semantics while expressing them in an API that
feels natural in Python and stays portable across backends.
