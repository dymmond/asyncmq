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
them while handlers run. Backends also record an active-claim timestamp when a
job is reserved, so a worker that exits before writing its first heartbeat can
still be recovered once the claim is older than `stalled_threshold`. A recovery
loop scans stale active claims and stale heartbeats, then releases matching jobs
back to runnable work.

For persistent backends, a killed worker process does not acknowledge or
complete its active job. The job remains active until its heartbeat or active
claim crosses the stalled threshold, then another recovery loop can release the
same active claim back to waiting work.

Transient backend errors during stalled scans, individual requeue operations, or
stalled-event emission are logged and the recovery loop continues. Jobs remain
eligible for later recovery attempts until a backend-owned lifecycle transition
successfully releases them.

Important limits:

- `InMemoryBackend` stalled recovery is process-local and does not survive
  process restart, but it still re-checks current active state before moving a
  stalled snapshot back to waiting.
- `RedisBackend` atomically re-checks canonical active state before requeueing
  a stalled snapshot, so recovery does not overwrite jobs that already reached
  a terminal state. Redis active lifecycle transitions also require the current
  active claim before writing completion, retry, defer, failure, or expiration.
  Recovered jobs re-enter the waiting queue through the same priority and FIFO
  score allocator used by ordinary enqueue.
- `PostgresBackend` conditionally updates still-active rows during stalled
  recovery, preserving terminal rows if the stalled snapshot is stale.
- `MongoDBBackend` conditionally updates still-active persisted documents
  during stalled recovery and updates local mirrors only after that transition.
- `RabbitMQBackend` publishes tokenized replacement deliveries during stalled
  recovery only after current metadata still matches the stalled active
  snapshot, and acknowledges stale broker redeliveries whose token no longer
  matches metadata state.
- Very long handlers should either finish within the visibility window or keep
  heartbeat renewal enabled.

## Ordering

Ordering is an eligibility guarantee, not a completion guarantee.

AsyncMQ dequeues eligible waiting jobs by priority first for the built-in
backends. Within the same priority, Redis and in-memory use insertion sequence,
MongoDB uses `created_at` plus `job_id`, PostgreSQL uses `created_at` with
unspecified ordering for exact timestamp ties, and RabbitMQ follows broker
ordering for ready deliveries. Once jobs are running, completion order can
differ because of concurrency, retries, delays, cancellation, worker crashes,
and stalled recovery.

Delayed jobs and retry backoff become eligible only after their scheduled time.
When they return to the waiting queue, they compete with other eligible waiting
jobs by the backend's priority and same-priority ordering rules.

RabbitMQ queues must be declared with priority support for priority ordering.
Existing RabbitMQ queues with incompatible declaration arguments must be
recreated or used with matching backend configuration.

## Cancellation

Cancellation is cooperative.

For waiting and delayed jobs, backends remove or mark the job so workers should
not start it. For active jobs, workers check cancellation before execution and
again immediately before invoking the handler; all built-in backends also guard
late lifecycle writes from overwriting cancellation. AsyncMQ cannot forcibly
interrupt arbitrary user code that is already executing outside the sandbox.

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
- MongoDB persists job documents, active heartbeats, and cancellation markers
  for cross-process recovery and cancellation checks.
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
