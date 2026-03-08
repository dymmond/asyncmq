# BullMQ Parity

This page tracks the practical parity target between BullMQ and AsyncMQ.

AsyncMQ is intentionally not a Redis clone. When BullMQ relies on Redis-native
data structures, AsyncMQ prefers a backend-neutral contract first and lets each
backend implement the capability in its own storage model.

## Current Matrix

| Domain | BullMQ capability | AsyncMQ status | Notes |
| --- | --- | --- | --- |
| Core jobs | enqueue, delay, priority, retries, backoff, TTL, DLQ | Supported | Available across built-in backends. |
| Workers | concurrency, pause/resume, graceful shutdown, stalled recovery | Supported | Worker lifecycle and stalled checks are built in. |
| Scheduling | delayed jobs | Supported | `delayed_job_scanner(...)` is started by `run_worker(...)`. |
| Scheduling | repeatable jobs (`every`, cron) | Supported | Local schedules use `Queue.add_repeatable(...)`; durable schedules use `Queue.upsert_repeatable(...)`. |
| Scheduling | durable schedule registry | Supported | Backends can persist repeatable definitions and workers poll them automatically. |
| Flows | parent/child dependencies | Supported | `FlowProducer` plus backend dependency tracking. |
| Queue admin | pause/resume, list jobs, clean, retry/remove failed jobs | Supported | Exposed through queue, CLI, and dashboard surfaces. |
| Events/telemetry | lifecycle events, dashboard metrics | Supported | Local emitter plus dashboard SSE/metrics history. |
| Multi-backend design | non-Redis operation | Supported | Redis, Postgres, MongoDB, RabbitMQ, and in-memory backends are supported. |

## BullMQ Mapping

| BullMQ concept | AsyncMQ equivalent |
| --- | --- |
| `Queue.add(...)` | `Queue.add(...)` / `Queue.enqueue(...)` |
| `Queue.addBulk(...)` | `Queue.add_bulk(...)` |
| Repeatable jobs / job schedulers | `Queue.upsert_repeatable(...)` |
| Process-local bootstrap schedules | `Queue.add_repeatable(...)` |
| `Worker` concurrency / lifecycle | `Worker` and `run_worker(...)` |
| Flow producer | `FlowProducer` |

## Intentional Differences

- AsyncMQ keeps repeatable scheduling backend-neutral instead of tying the API to Redis scripts.
- `Queue.add_repeatable(...)` remains a local worker bootstrap mechanism for code-defined schedules.
- Durable repeatables are implemented through backend storage capabilities, not a single Redis-only scheduler format.

## Remaining Gaps

The parity focus is practical rather than byte-for-byte API identity. Areas that
still need deeper BullMQ-style coverage should be treated as follow-up work:

- richer queue getter/count pagination APIs
- broader deduplication policies and conflict semantics
- stronger multi-worker coordination for scheduler ownership in every backend
