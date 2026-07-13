# Production Operations

This guide focuses on how AsyncMQ behaves once it stops being a local
development tool and starts carrying real traffic.

The themes are the same ones you would expect from BullMQ in production:

- queue partitioning
- worker topology
- retries and idempotency
- delayed and repeatable correctness
- incident response
- backend-specific tradeoffs

## Recommended Deployment Model

For most teams, the cleanest production topology is:

1. application processes that only produce jobs
2. dedicated worker processes or containers per queue family
3. one optional stalled-recovery process when stalled recovery is enabled
4. dashboard and admin surfaces as separate operational services

Avoid embedding high-volume workers into the same process that handles your
user-facing HTTP traffic unless you are deliberately optimizing for a small
deployment footprint.

For Docker and Kubernetes examples, see [Deployment](deployment.md).

For dashboard deployments, wire platform probes to:

- `/health` for process liveness
- `/ready` for backend reachability and inspection readiness

## Queue Design

Split queues by operational profile, not by arbitrary code ownership.

Good queue boundaries:

- `emails`: moderate latency tolerance, external provider throttling
- `webhooks`: bursty I/O work, retry-heavy
- `billing`: low concurrency, stronger operator scrutiny
- `media`: CPU-heavy or long-running tasks

Bad queue boundaries:

- one queue per micro-feature
- one giant queue for unrelated workloads with different SLAs

The goal is to make concurrency, rate limiting, alerting, and incident
containment predictable.

## Backend Selection

Choose the backend based on operational guarantees, not familiarity alone.

### Redis

Best default production backend when you want:

- strong shared coordination
- durable repeatable schedules
- distributed deduplication and scheduler locks
- a simple operating model

### Postgres

Good fit when you want:

- SQL-native persistence
- advisory-lock-based coordination
- durable schedules without running Redis

Bootstrap the schema before first use.

### MongoDB

Good fit when your platform is already document-centric, but remember:

- coordination locks are process-local
- some queue-control guarantees are intentionally weaker across multiple processes

### RabbitMQ

Best when AMQP broker delivery is the requirement, but remember:

- AsyncMQ still needs a metadata store for job state, results, schedules, and locks
- operational quality depends partly on that metadata store

### In-memory

Use only for:

- local development
- tests
- ephemeral demos

It is intentionally not a durable production backend.

## Scaling Workers

Scale with a combination of:

- queue-level concurrency
- more worker processes
- queue separation by workload type

Example thought process:

- I/O-heavy email/webhook jobs: higher concurrency, higher worker count
- CPU-heavy PDF generation: lower concurrency, often isolated workers
- rate-limited downstream API: dedicated queue with moderate concurrency plus rate limiting

Remember:

- concurrency is per worker process
- rate limiting is per worker process
- backend coordination quality varies by backend

## Idempotency and Retries

AsyncMQ gives you retries, but it does not give you exactly-once execution.

Plan for:

- worker crash before result persistence
- retry after transient failure
- manual operator replay
- stalled recovery re-enqueue

Production-safe handlers typically use:

- database upserts
- external idempotency keys
- state-machine checks before mutating downstream systems
- deterministic output paths

If the side effect matters, idempotency belongs in task logic, not only in the
queue runtime.

## Delayed and Repeatable Work

For production scheduling:

- use `Queue.upsert_repeatable(...)` for durable schedules
- reserve `Queue.add_repeatable(...)` for local code-owned schedules
- keep `scan_interval` low enough for acceptable latency
- verify scheduler ownership guarantees for your chosen backend

For deduplicated delayed work:

- align `delay` and deduplication TTL windows
- use `replace=True` only when replacing older delayed work is actually correct

## Observability

At minimum, watch:

- waiting count
- active count
- delayed count
- failed count
- DLQ growth
- worker heartbeat freshness
- repeatable next-run drift

Practical surfaces:

- [Dashboard](../dashboard/dashboard.md)
- [Queue inspection APIs](../features/queues.md#inspection-and-admin-apis)
- [CLI reference](../reference/cli-reference.md)
- `/metrics/prometheus` for Prometheus-compatible scrape metrics
- `structured_logging=True` for JSON log ingestion
- optional OpenTelemetry worker spans with queue, job id, task, priority,
  retry count, completion status, and exception type attributes

If you need durable long-term analytics, export metrics and audit information
to your observability stack. AsyncMQ's built-in dashboard history is aimed at
operations, not long-term warehouse analytics.

## Incident Playbooks

### Backlog rising

1. inspect waiting counts and worker heartbeats
2. confirm the queue is not paused
3. sample active jobs and failed jobs
4. scale workers or reduce upstream production pressure
5. pause the queue only when containment is worth the backlog cost

### Failure spike

1. inspect recent failures and traceback patterns
2. identify whether the root cause is code, dependency, or payload-specific
3. patch or roll back
4. retry a controlled subset first
5. expand retries only after the error rate stabilizes

### Stalled jobs

1. confirm `enable_stalled_check=True`
2. confirm workers are running through `run_worker(...)`, `Queue.run()`, or `Worker.run()`
3. verify long-running handlers are still renewing job heartbeats

## Retention and Cleanup

Use queue admin APIs as part of operations hygiene:

```python
await queue.clean_jobs(grace=3600, limit=1000, state="completed")
await queue.clean_jobs(grace=86400, limit=1000, state="failed")
await queue.drain(include_delayed=True)
```

The CLI exposes the same queue-admin controls:

```bash
asyncmq queue clean emails --state completed --grace 3600 --limit 1000
asyncmq queue drain emails --include-delayed
asyncmq queue obliterate emails --force
```

Recommended pattern:

- keep enough completed jobs for recent debugging
- keep failed jobs long enough for incident review
- drain or obliterate only with clear operator intent

`obliterate(force=True)` is intentionally destructive. Treat it as an incident
or environment reset tool, not a daily maintenance command.

## Safe Rollouts

Before deploying worker changes:

1. confirm new task modules are imported by the worker runtime
2. confirm payload schema changes remain backward compatible with queued jobs
3. confirm retry/backoff changes are acceptable for in-flight jobs
4. confirm repeatable schedules will not duplicate work after restart

For rolling worker restarts, prefer this order:

1. pause the queue only when you need a queue-wide intake stop
2. signal each worker process to drain so it stops claiming new jobs
3. let in-flight jobs finish within the deployment drain window
4. terminate any process that exceeds the drain window and rely on stalled
   recovery/idempotent handlers for interrupted work
5. start the replacement workers and confirm heartbeat freshness

If a deploy changes task semantics in a non-backward-compatible way, drain or
migrate queued jobs explicitly rather than hoping the old payload shape still
works.

## Backend Caveats to Remember

- Redis and Postgres give the strongest built-in coordination.
- MongoDB and in-memory intentionally do not pretend to be distributed lock services.
- RabbitMQ coordination depends on the metadata store choice.
- Queue pause, repeatable ownership, and deduplication are only as strong as
  the backend implementation behind them.

## Related

- [Workers](../features/workers.md)
- [Schedulers](../features/schedulers.md)
- [Queues](../features/queues.md)
- [Troubleshooting](../troubleshooting.md)
- [Backend Capabilities](../reference/backend-capabilities.md)
