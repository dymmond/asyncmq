# Delivery Semantics

This page is the canonical statement of AsyncMQ delivery behavior.

AsyncMQ is an at-least-once task queue. It is not an exactly-once execution
system. Handlers must be idempotent whenever side effects matter.

## Delivery Guarantee

AsyncMQ attempts to ensure that an accepted job is eventually made available to
workers until it reaches a terminal state such as `completed`, `failed`, or
`expired`.

The runtime can deliver the same logical job more than once when:

- a worker crashes after the handler side effect but before completion is
  durably recorded
- stalled recovery releases an active job whose heartbeat stopped
- an operator manually retries a failed job
- a backend or broker redelivers after a connection loss
- a task raises after partially completing external work

Exactly-once effects must be implemented in task code with database upserts,
external idempotency keys, state-machine checks, or other domain controls.

## Acknowledgements

Workers do not treat handler return as sufficient by itself. Completion, retry,
expiration, cancellation, and terminal failure are routed through backend-owned
lifecycle transitions where the backend can update state, result/error fields,
acknowledgement, delayed placement, and DLQ placement in the safest way its
storage model supports.

If a backend cannot make a transition fully transactional across all systems it
uses, AsyncMQ still exposes at-least-once behavior rather than pretending to
offer exactly-once execution.

## Visibility and Stalled Recovery

When `enable_stalled_check=True`, workers write active-job heartbeats and renew
them while handlers run. A recovery loop scans for active jobs whose heartbeat is
older than `stalled_threshold` and releases them back to runnable work.

Important limits:

- `InMemoryBackend` stalled recovery is process-local and does not survive
  process restart.
- RabbitMQ recovery relies on broker redelivery for unacknowledged messages
  after restart and uses the metadata store to reset job state.
- Very long handlers should either finish within the visibility window or keep
  heartbeat renewal enabled.

## Ordering

Ordering is an eligibility guarantee, not a completion guarantee.

AsyncMQ dequeues eligible waiting jobs by priority first and FIFO order within
the same priority for the built-in backends. Once jobs are running, completion
order can differ because of concurrency, retries, delays, cancellation, worker
crashes, and stalled recovery.

Delayed jobs and retry backoff become eligible only after their scheduled time.
When they return to the waiting queue, they compete with other eligible waiting
jobs by the backend's priority/FIFO rules.

RabbitMQ queues must be declared with priority support for priority ordering.
Existing RabbitMQ queues with incompatible declaration arguments must be
recreated or used with matching backend configuration.

## Cancellation

Cancellation is cooperative.

For waiting and delayed jobs, backends remove or mark the job so workers should
not start it. For active jobs, workers check cancellation before execution and
again immediately before invoking the handler. AsyncMQ cannot forcibly interrupt
arbitrary user code that is already executing outside the sandbox.

## Expiration

TTL is a staleness window measured from `created_at`.

If a worker sees an expired job, it marks the job `expired`, acknowledges the
active ownership, emits `job:expired`, and moves the payload to the DLQ. TTL is
not a hard execution timeout for a handler that has already started.

## Backend Caveats

Redis, PostgreSQL, MongoDB, RabbitMQ, and InMemory backends share the same public
contract, but their fault domains differ:

- Redis uses Redis data structures and scripts for lifecycle transitions.
- PostgreSQL uses database transactions for lifecycle transitions.
- MongoDB persists job documents and active heartbeats for restart recovery.
- RabbitMQ uses RabbitMQ for broker delivery and a metadata store for job state.
- InMemory is for local development, tests, and demos; it is not durable.

Check [Backend Capabilities](backend-capabilities.md) before relying on a
specific operational behavior in multi-process or multi-machine deployments.

## Operator Checklist

- Make task handlers idempotent.
- Use `job_id` or deduplication for producer-side duplicate suppression.
- Enable stalled recovery for production workers that need visibility timeout
  behavior.
- Monitor waiting, active, failed, delayed, DLQ size, and worker heartbeats.
- Use bounded retries and backoff for external dependencies.
- Treat manual retry and DLQ replay as duplicate execution.
