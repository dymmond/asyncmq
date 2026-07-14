# BullMQ Parity

This page tracks the practical parity target between BullMQ and AsyncMQ.

AsyncMQ is intentionally not a Redis clone. When BullMQ relies on Redis data
structures, AsyncMQ prefers a portable backend contract first and lets each
backend implement the capability in its own storage model.

If you are actively migrating an existing BullMQ codebase, start with the
[BullMQ Migration Guide](bullmq-migration.md) and use this page as the
capability status summary.

## Current Matrix

| Domain | BullMQ capability | AsyncMQ status | Notes |
| --- | --- | --- | --- |
| Core jobs | enqueue, delay, priority, retries, backoff, TTL, DLQ | Supported | Available across built-in backends. |
| Producers | custom `jobId` duplicate suppression | Supported | `Queue.add(..., job_id=...)` and `Queue.add_bulk(...)` ignore duplicates within a queue until the original job is removed. |
| Producers | deduplication / debounce / throttle | Supported | `Queue.add(..., deduplication=...)`, `debounce=...`, `get_deduplication_job_id(...)`, and `remove_deduplication_key(...)` are available. |
| Workers | concurrency, pause/resume, graceful shutdown, stalled recovery | Supported | Worker lifecycle and stalled checks are built in. |
| Scheduling | delayed jobs | Supported | `delayed_job_scanner(...)` is started by `run_worker(...)`. |
| Scheduling | repeatable jobs (`every`, cron) | Supported | Local schedules use `Queue.add_repeatable(...)`; durable schedules use `Queue.upsert_repeatable(...)`. |
| Scheduling | durable schedule registry | Supported | Backends can persist repeatable definitions and workers poll them automatically. |
| Scheduling | scheduler ownership | Supported | Repeatables stored in the backend advance under a per-queue scheduler lock. |
| Flows | parent/child dependencies | Supported | `FlowProducer` plus backend dependency tracking. |
| Queue admin | pause/resume, getters, counts, pagination, drain, clean, obliterate | Supported | `Queue.get_job(...)`, `get_jobs(...)`, `get_job_counts(...)`, `drain(...)`, `clean_jobs(...)`, and `obliterate(...)` are available across built-in backends. |
| Events/telemetry | lifecycle events, dashboard metrics | Supported | Local emitter plus dashboard SSE/metrics history. |
| Multi-backend design | non-Redis operation | Supported | Redis, Postgres, MongoDB, RabbitMQ, and in-memory backends are supported. |

## BullMQ Mapping

| BullMQ concept | AsyncMQ equivalent |
| --- | --- |
| `Queue.add(...)` | `Queue.add(...)` / `Queue.enqueue(...)` |
| `Queue.addBulk(...)` | `Queue.add_bulk(...)` |
| `Queue.getJob(...)` | `Queue.get_job(...)` |
| `Queue.getJobs(...)` | `Queue.get_jobs(...)` |
| `Queue.getJobCounts(...)` | `Queue.get_job_counts(...)` |
| `Queue.getJobCountByTypes(...)` | `Queue.get_job_count_by_types(...)` |
| `Queue.count(...)` | `Queue.count(...)` |
| `Queue.drain(...)` | `Queue.drain(...)` |
| `Queue.clean(...)` | `Queue.clean_jobs(...)` |
| `Queue.obliterate(...)` | `Queue.obliterate(...)` |
| `Queue.getDeduplicationJobId(...)` | `Queue.get_deduplication_job_id(...)` |
| `Queue.removeDeduplicationKey(...)` | `Queue.remove_deduplication_key(...)` |
| Repeatable jobs / job schedulers | `Queue.upsert_repeatable(...)` |
| Process-local bootstrap schedules | `Queue.add_repeatable(...)` |
| `Worker` concurrency / lifecycle | `Worker` and `run_worker(...)` |
| Flow producer | `FlowProducer` |

## Intentional Differences

- AsyncMQ keeps repeatable scheduling portable across backends instead of tying the API to Redis scripts.
- `Queue.add_repeatable(...)` remains a local worker bootstrap mechanism for schedules defined in code.
- Durable repeatables are implemented through backend storage capabilities, not a single Redis scheduler format.
- AsyncMQ exposes a derived `waiting-children` inspection bucket from dependency metadata, but it does not persist BullMQ's separate Redis `paused` or `prioritized` job buckets. Queue pause remains queue-level control, and priority remains ordering metadata on waiting jobs.
- AsyncMQ uses Pythonic `job_id=` instead of BullMQ's camelCase `jobId` on `Queue.add(...)`, but `Queue.add_bulk(...)` accepts both `job_id` and `jobId` keys for migration convenience.
- AsyncMQ stores deduplication metadata on job payloads instead of Redis specific side keys, which keeps the feature portable across built-in backends.
- Distributed scheduler ownership and deduplication locking are strongest on Redis and Postgres. In-memory and MongoDB provide process-local coordination because they intentionally avoid pretending to be distributed lock services.

## Remaining Gaps

The parity focus is practical rather than byte-for-byte API identity.

Remaining differences are intentional rather than missing implementations:

- AsyncMQ exposes dashboard-oriented metrics history and SSE telemetry instead of BullMQ's Redis-stream queue-events and metrics getter API surface.
- Metrics and audit history are currently process-local dashboard stores, so long-term multi-instance analytics should be exported to external observability tooling.
