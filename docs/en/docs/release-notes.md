# Release Notes

## 0.9.0

### Added

- Benchmark planning now exposes a canonical workload matrix, measurement
  policy, and competitor availability check for Celery, Dramatiq, Arq, RQ, and
  Huey before performance results are recorded.
- Workers now expose cooperative drain controls so application-owned shutdown
  paths can stop claiming new jobs, let in-flight jobs finish, deregister, and
  run shutdown hooks before process termination.
- `asyncmq worker start` now requests cooperative drain on SIGINT and SIGTERM.
- Dashboard deployments now expose `/metrics/prometheus` with scrapeable queue,
  worker, and readiness gauges for Prometheus-compatible monitoring.
- Public producer APIs now enforce a configurable JSON-encoded job payload size
  guard through `max_job_payload_bytes` to reduce resource-exhaustion risk from
  oversized jobs.
- Queue administration commands now expose drain, clean, and obliterate
  operations through the CLI for incident response and controlled maintenance.
- Delivery semantics are now documented in one reference page covering
  at-least-once behavior, duplicate delivery, visibility recovery, ordering,
  cancellation, expiration, and backend caveats.
- Deployment guidance now covers process separation, Docker images, Kubernetes
  probes, rolling worker restarts, backend readiness, and recovery expectations.
- Benchmark tooling now includes a parameterized AsyncMQ in-memory load runner
  that emits JSON throughput, latency, CPU, and max RSS measurements for local
  regression checks.
- Worker execution can now emit optional OpenTelemetry spans with queue, task,
  retry, priority, status, and exception attributes when tracing is enabled.
- Dashboard authentication now includes an authorization gate for admin users
  and optional role allowlists.
- Built-in logging can now emit JSON records through `structured_logging=True`.
- Worker CLI operations now include single-worker inspection by worker ID.

### Fixed

- Worker dequeue now respects local execution capacity before claiming jobs,
  preventing a worker process from holding more active jobs than its configured
  concurrency can execute. Full workers leave unreserved waiting jobs available
  to other workers instead of hiding backlog in local reservations.
- Worker completion, retry, expiration, cancellation, and terminal-failure paths
  now route through backend-owned lifecycle transitions so backends can make
  state, result, acknowledgement, delay, and DLQ updates atomic for their
  storage technology.
- Redis now applies worker lifecycle transitions through server-side scripts,
  keeping active ownership, heartbeat cleanup, canonical job payloads, delayed
  placement, and DLQ placement in one Redis execution.
- PostgreSQL now applies worker lifecycle transitions through database
  transactions, preventing retry/defer paths from being removed by
  acknowledgement cleanup and preserving terminal job rows for inspection.
- MongoDB now applies worker lifecycle transitions through backend-owned
  document updates that keep completion, retry/defer, expiration, failure, and
  local runtime mirrors aligned.
- RabbitMQ now applies worker lifecycle transitions through backend-owned paths
  that save metadata before broker acknowledgement and route failures through
  DLQ publishing before acknowledgement.
- Stalled-job visibility now renews job heartbeats while handlers run, and the
  normal worker entrypoints start stalled recovery automatically when
  `enable_stalled_check=True`.
- Stalled recovery now validates active-job release from real dequeued jobs,
  including local RabbitMQ in-flight delivery acknowledgement during recovery.
- PostgreSQL, MongoDB, and RabbitMQ waiting-job dequeue now respects priority
  first and FIFO order within the same priority.
- Delayed-job scanning now delegates due-job promotion to backend-owned
  transitions instead of removing delayed jobs and re-enqueueing them from the
  scanner loop.
- Retry and DLQ payloads now retain `last_error` and `error_traceback` so
  operators can inspect the failure cause after the worker lifecycle transition.
- Manual retry operations now requeue clean `waiting` payloads across backends,
  clearing stale result and failure fields before the next execution attempt.
- PostgreSQL cancellation now records the cancellation and removes matching
  waiting or delayed rows in one transaction so cancelled jobs no longer remain
  inspectable as eligible work.
- Dependency flow creation now keeps unresolved children in `waiting-children`
  instead of runnable waiting queues across the built-in backends, and
  dependency resolution promotes children only after the last parent completes.
- Worker execution now stamps `last_attempt` when a handler attempt starts, so
  completed, retried, and failed payloads expose the most recent execution time.
- MongoDB now persists active-job heartbeat timestamps in job documents and
  indexes active heartbeat scans, allowing a separate recovery process to
  release stale active jobs after the original worker process exits.
- RabbitMQ stalled recovery now discovers queues from persisted metadata after
  backend restart and avoids publishing a duplicate recovery message when
  RabbitMQ broker redelivery already owns the unacknowledged delivery.
- Dashboard CORS is now opt-in by explicit origin allowlist, with
  wildcard-plus-credentials configurations rejected during admin app startup.
- Authenticated dashboard mutation requests now enforce same-origin `Origin`
  checks by default to reduce CSRF exposure on queue and job operations.
- `JWTAuthBackend` now rejects missing or shorter-than-32-byte HMAC secrets at
  startup instead of accepting weak HS* signing keys.
- Dashboard deployments now expose `/health` for liveness and `/ready` for
  backend reachability/readiness checks.
- CLI group callbacks now use Click's forward-compatible subcommand detection
  instead of deprecated `protected_args` access.
- Sandbox subprocess execution now defaults to the `spawn` multiprocessing
  context to avoid unsafe `fork` behavior in multi-threaded runtimes.
- Sandbox timeouts now fail closed by default instead of re-running timed-out
  handlers in the parent worker process; fallback remains available only when
  callers explicitly opt in.
- PostgreSQL, MongoDB, and RabbitMQ queue pause/resume state is now persisted in
  backend storage so separate worker and admin processes observe the same queue
  control state during restarts and rolling operations.
- MongoDB waiting dequeue and delayed-job promotion now use MongoDB document
  state transitions, allowing separate producer and worker backend instances to
  share immediate and delayed jobs instead of relying on process-local mirrors.
- RabbitMQ stalled recovery now publishes tokenized replacement deliveries when
  the original broker delivery is still held by another connection, while stale
  broker redeliveries are acknowledged and ignored after recovery.
- Active job claims now record claim timestamps across built-in backends, so
  stalled recovery can release jobs that were reserved by a worker that exited
  before writing its first heartbeat.
- Worker dequeue now waits for both local execution capacity and configured
  rate-limit tokens before claiming a job, keeping rate-limited backlog visible
  to other workers.
- RabbitMQ queue draining now follows the shared backend contract, including
  delayed-job removal when requested and returning removed job identifiers.
- RabbitMQ failed-job retry now removes the matching ready DLQ broker delivery
  before publishing the retry to the main queue.
- RabbitMQ job removal now clears matching ready broker deliveries from the
  main queue and DLQ, and acknowledges locally owned active deliveries.
- Queue cleanup now removes jobs through backend removal paths so legacy
  `clean(...)` operations keep queue membership, broker deliveries, and
  inspection metadata aligned.
- In-memory waiting queues now avoid full-list sorting on every unique enqueue
  and use constant-time dequeue from the hot path while preserving priority and
  FIFO ordering. Active completion, retry/defer, and DLQ transitions also avoid
  scanning waiting and delayed queues when the job is already owned by a worker.

## 0.8.1

### Fixed

- Restored package-level resolution for the `asyncmq.jobs` submodule when AsyncMQ's lazy exports are active, preventing dotted imports such as `asyncmq.jobs.Job` from failing after import-order edge cases on Python 3.11.

## 0.8.0

### Added

- BullMQ-style producer parity for practical Python usage:
    - queue-scoped `job_id` duplicate suppression on `Queue.add(...)` and `Queue.add_bulk(...)`,
    - BullMQ-style deduplication, throttle, and debounce semantics via `deduplication={...}` and `debounce={...}`,
    - deduplication inspection and control APIs such as `get_deduplication_job_id(...)`, `get_debounce_job_id(...)`, and `remove_deduplication_key(...)`.
- Expanded queue administration and inspection APIs:
    - `get_job(...)`,
    - `get_jobs(...)`,
    - `get_job_counts(...)`,
    - `get_job_count_by_types(...)`,
    - `count(...)`,
    - `remove_job(...)`,
    - `retry_job(...)`,
    - `drain(...)`,
    - `clean_jobs(...)`,
    - `obliterate(...)`,
    - BullMQ-style inspection helpers for waiting, delayed, completed, failed, active, and `waiting-children`.
- Durable repeatable scheduling APIs for production-oriented schedule management:
    - `upsert_repeatable(...)`,
    - `remove_repeatable(...)`,
    - durable backend-managed repeatable discovery across built-in backends.
- New documentation sections for:
    - deduplication,
    - BullMQ parity,
    - backend capability differences,
    - production operations,
    - BullMQ migration guidance.

### Changed

- Brought AsyncMQ to practical parity while preserving AsyncMQ's backend-neutral architecture rather than coupling behavior to Redis-only data structures.
- Queue producer semantics now behave consistently across single-job and bulk-job creation, including custom job identifiers, deduplication windows, delayed replacement, and duplicate suppression.
- Repeatable scheduling now supports both local code-defined schedules and durable backend-managed schedules in one coherent runtime model.
- Scheduler ownership for durable repeatables is now coordinated under queue-scoped locks so multiple workers do not all advance the same backend schedule at once.
- Documentation was substantially expanded and reorganized:
  - deeper runtime guides for jobs, workers, schedulers, and flows,
  - richer production and migration guidance,
  - clearer navigation between features, reference material, and operations documentation.

### Fixed

- Sandbox execution integration now respects the configured sandbox handler path consistently during worker execution.
- PostgreSQL job identity semantics are now queue-scoped, aligning custom `job_id` handling  duplicate suppression behavior.
- Retry and job-payload persistence paths were aligned across backends so stateful metadata such as deduplication, dependency updates, and retried payload state are preserved correctly.
- MongoDB payload replacement now removes stale job metadata fields instead of leaving outdated values behind after payload mutation.
- RabbitMQ metadata persistence and locking fallbacks were aligned with the shared backend contract for queue inspection, schedule management, and deduplication-aware updates.

## 0.7.0

### Added

- Migrated the CLI to a Sayer-backed implementation while preserving existing command names and behavior.
- Added a shared dashboard queue-count aggregation helper for consistent overview/metrics/SSE data.
- Added richer dashboard operations features:
  - queue job filtering/search (`q`, `task`, `job_id`, sorting),
  - action audit trail page (`/audit`),
  - metrics history endpoint (`/metrics/history`) and richer metrics history visualizations.
- Added high-value regression tests for:
    - MongoDB store `_id` update behavior,
    - worker handling of backend-wrapped payloads,
    - dashboard count aggregation and repeatables actions,
    - dashboard audit/history stores and filtering behavior,
    - project metadata validation for optional dependency extras.

### Changed

- Expanded and restructured documentation with:
    - a rewritten main entrypoint (`index.md`),
    - richer feature docs,
    - new how-to guides,
    - new reference section,
    - dedicated troubleshooting documentation,
    - improved dashboard operator documentation (capabilities + operations playbook).
- Updated docs navigation for clearer onboarding and operational workflows.
- Improved dashboard controllers to reuse consistent queue/job state aggregation and safer backend fallbacks.
- Expanded dashboard documentation with route reference, richer runbooks, and additional architecture/workflow diagrams.

### Fixed

- RabbitMQ backend acknowledgment lifecycle:
  - dequeue no longer auto-acks,
  - in-flight messages are tracked and acknowledged explicitly via `ack(...)`.
- RabbitMQ delayed/repeatable and queue-state consistency:
  - normalized delayed-state handling,
  - corrected delayed listing/removal semantics,
  - improved queue pause/resume and worker heartbeat visibility behavior.
- Dependency and stalled-job edge cases:
  - improved dependency merge/resolution behavior,
  - normalized stalled-job payload handling across worker/recovery paths.
- DLQ terminal-state correctness across backends:
  - preserved explicit terminal statuses (`failed` / `expired`) instead of forcing non-terminal values.
- CLI queue-info correctness:
  - now prefers backend `queue_stats(...)` instead of backend-internal attributes.
- Packaging extras correctness:
  - fixed `asyncmq[all]` to include concrete installable dependencies.
- Typing fix in repeatable queue definitions (`Queue.add_repeatable`) to satisfy strict type checking.
- Worker completion path now tolerates minimal backend stubs that do not implement dependency-resolution APIs.
- PostgreSQL delayed-job retrieval now removes due delayed records atomically, restoring delayed lifecycle correctness.
- In-memory and MongoDB backends now prevent failed jobs from being double-counted as waiting in queue stats.
- Stalled-job recovery behavior was aligned across backends for payload/state consistency in re-enqueue flows.


## 0.6.3

### Changed

- Update internals to start using Palfrey for the dashboard.

## 0.6.2

### Fixed

- When using the settings it was causing a conflict with the types and not casting properly to the right type due
to the `from future import __annotations__`.

## 0.6.1

### Fixed

- Mininum requirements for RabbitMQ.

## 0.6.0

### Added

- Implemented `AuthGateMiddleware` to protect dashboard routes and handle HTMX redirects.
- Added CORS and session middleware support in `AsyncMQAdmin` with customizable options.
- Integrated `DashboardConfig` access via global settings (`settings.dashboard_config`).
- Introduced `JWTAuthBackend` and database-backed authentication backends for enhanced security.
- Expanded documentation with detailed examples for custom `AuthBackend` implementations and integration guides.

### Changed

- Updated the [Dashboard](./dashboard/dashboard.md) documentation to reflect new authentication flows and backend integrations.
- Clarified upgrade instructions to assist users transitioning from previous dashboard versions.
- Improved authentication-related documentation for better clarity and usability.

#### Breaking

Check the documentation of the [Dashboard](./dashboard/dashboard.md) as now with the introduction of the new flow, the old
way won't work anymore. The `dashboard` is no longer available and now everything is done via `AsyncMQAdmin` object.

The documentation explains how to easily integrate and update (its not too much different)

## 0.5.1

### Add

- `send()` as alternative to `enqueue`.

### Changed

- Allow `enqueue`, `enqueue_delayed` and `delay` to return the job id directly.

## 0.5.0

### Added

- Parallel DAG execution fixes for backend via `FlowProducer` and `process_job`.
- Backward-compatible serialization improvements for `Job` to support both `task_id` and legacy `task` fields.
- Automatic dependency resolution fallback when atomic flow addition is not supported by the backend.
- Lifecycle hooks for worker startup and shutdown via `worker_on_startup` and `worker_on_shutdown`, supporting both sync and async callables.
- New reusable `lifecycle.py` module providing utilities like `normalize_hooks`, `run_hooks`, and `run_hooks_safely`.
- New section in the [workers for hooks](./features/workers.md) and lifecycle.

### Changed

- Improved worker and flow orchestration stability under mixed backends.
- Worker startup now runs `worker_on_startup` hooks before registering heartbeats.
- Worker shutdown now runs `worker_on_shutdown` hooks safely after deregistration.

### Fixed

- Timeout issues when running parallel DAG flows due to backend event-loop mismatches.
- Various minor concurrency edge cases in job scanning and dependency unlocking logic.
- Fix `BaseSettings` for python 3.14 and more complex inheritances.
- Sandbox task discovery.

## 0.4.6

### Fixed

Due to some internals, Lilya was missing for the dashboard dependency and causing a regression.

- Added Lilya as dependency for AsyncMQ and remove several redundant others.

## 0.4.5

### Fixed

- Lazy loading of the settings via Monkay on stores.

## 0.4.4

### Fixed

- Lazy loading of the settings via Monkay.

## 0.4.3

### Added

- Python 3.14 support.

## 0.4.2

### Added

- Add config option for custom JSON loads/dumps [#73](https://github.com/dymmond/asyncmq/pull/73) by [chbndrhnns](https://github.com/chbndrhnns).
- No negative numbers on workers list pagination [#69](https://github.com/dymmond/asyncmq/pull/69) by [chbndrhnns](https://github.com/chbndrhnns).

### Changed

- Accept a Redis client instance for RedisBackend [#67](https://github.com/dymmond/asyncmq/pull/67) by [chbndrhnns](https://github.com/chbndrhnns).

### Fixed

- Lazy loading for the global settings was pointing to the wrong location.
- Error when parsing Boolean values from environment variables.
- Do not reload dashboard test files [#64](https://github.com/dymmond/asyncmq/pull/64) by [chbndrhnns](https://github.com/chbndrhnns).
- No negative numbers on workers list pagination [#68](https://github.com/dymmond/asyncmq/pull/68) by [chbndrhnns](https://github.com/chbndrhnns).

## 0.4.1

### Changed

In the past, AsyncMQ was using `dataclass` to manage all the settings but we found out that can be a bit cumbersome for a lot
of people that are more used to slightly cleaner interfaces and therefore, the internal API was updated to stop using `@dataclass` and
use directly a typed `Settings` object.

- Replace `Settings` to stop using `@dataclass` and start using direct objects instead.

**Example before**

```python
from dataclasses import dataclass, field
from asyncmq.conf.global_settings import Settings


@dataclass
class MyCustomSettings(Settings):
    hosts: list[str] = field(default_factory=lambda: ["example.com"])
```

**Example after**

```python
from asyncmq.conf.global_settings import Settings


class MyCustomSettings(Settings):
    hosts: list[str] = ["example.com"]
```

This makes the code cleaner and readable.

## 0.4.0

### Added

- Address session middleware from the dashboard config and can be changed.
- Make cli commands more consistent across the client.
- `start_worker` now uses the `Worker` object. This was previously added but never 100% plugged into the client.

### Fixed

- Register/De-register workers in `run_worker`.
- CLI: Catch RuntimeError when running commands via anyio
- Log exceptions during job execution.
- Out-of-process workers do not pick up jobs from the queue
- `index.html` had a typo for the `<script />`

## 0.3.1

### Added

* **RabbitMQBackend**
    * Full implementation of the `BaseBackend` interface over AMQP via `aio-pika`:
        * `enqueue` / `dequeue` / `ack`
        * Dead-letter queue support (`move_to_dlq`)
        * Delayed jobs (`enqueue_delayed`, `get_due_delayed`, `list_delayed`, `remove_delayed`)
        * Repeatable jobs (`enqueue_repeatable`, `list_repeatables`, `pause_repeatable`, `resume_repeatable`)
        * Atomic flows & dependency resolution (`atomic_add_flow`, `add_dependencies`, `resolve_dependency`)
        * Queue pause/resume, cancellation/retry, job-state/progress/heartbeat tracking
        * Broker-side stats (`queue_stats`) and queue draining (`drain_queue`)

* **RabbitMQJobStore**
    * Implements `BaseJobStore`, delegating persistence to any other store (e.g. `RedisJobStore`), so metadata stays flexible.

### Changed

* Unified on the default exchange for enqueueing and DLQ publishing

### Fixed

* Proper wrapping of store results into `DelayedInfo` and `RepeatableInfo` dataclasses
* Correct scheduling semantics using epoch timestamps (`time.time()`)

## 0.3.0

### Fixed

- Enqueue/delay was not returning a job id.
- Backend was not returning the id on enqueue.

#### Breaking change

* This barely affects you but the order of parameters when the `.enqueue(backend, ...)` happens, it is now
```.enqueue(..., backend=backend)```

##### Example

**Before**:

```python
import anyio
from asyncmq.queues import Queue

async def main():
    q = Queue("emails")
    await send_welcome.enqueue(q.backend, "alice@example.com", delay=10)
anyio.run(main)
```

**Now**

```python
import anyio
from asyncmq.queues import Queue

async def main():
    q = Queue("emails")
    await send_welcome.enqueue("alice@example.com", backend=q.backend, delay=10)
anyio.run(main)
```

## 0.2.3

### Fixed

- Add alternative check to get the `concurrency` field from the Redis backend.
- Ensure pagination data is checked when listing workers.
- Added check for `workers.html` pagination to ensure it is not empty.

## 0.2.2

### Changed

- Updated minimum version of Lilya.
- Workers controller now reflects pagination and sizes.

### Fixed

- When starting a worker from the command-line, it was not automatically registering that same worker.
- The `workers.html` was not reflecting the correct information about the active workers.

## 0.2.1

### Fixed

- StaticFiles were not enabling the html to True for the dashboard.

## 0.2.0

## Release Notes

### Added

* **Dashboard**

    * Brand new [Dashboard](./dashboard/dashboard.md) where it shows how to integrate with your AsyncMQ. This dahboard is still
in *beta* mode so any feedback is welcomed.

* **Top-Level CLI Commands**
  Added four new top-level commands to the `asyncmq` CLI (powered by Click & Rich):

    * `list-queues` — list all queues known to the backend
    * `list-workers` — show all registered workers with queue, concurrency, and last heartbeat
    * `register-worker <worker_id> <queue> [--concurrency N]` — register or bump a worker's heartbeat **and** concurrency
    * `deregister-worker <worker_id>` — remove a worker from the registry.
    * `AsyncMQGroup` to `cli` ensuring the `ASYNCMQ_SETTINGS_MODULE` is always evaluated beforehand.

* **Worker Configuration**

    * `Worker` now accepts a `heartbeat_interval: float` parameter (default `HEARTBEAT_TTL/3`) to control how often it re-registers itself in the backend.

### Changed

* **Redis Backend: Store Concurrency**

    * Now `register_worker` stores both `heartbeat` and `concurrency` as a JSON blob in each hash field.
    * `list_workers` parses that JSON so the returned `WorkerInfo.concurrency` reflects the actual setting.

* **RedisBackend.cancel_job**

    * Now walks the **waiting** and **delayed** sorted sets via `ZRANGE`/`ZREM` instead of using list operations, then marks the job in a Redis `SET`.
    * Eliminates “WRONGTYPE” errors when cancelling jobs.

* **InMemoryBackend.remove_job**

    * Expanded to purge a job from **waiting**, **delayed**, **DLQ**, and to clean up its in-memory state (`job_states`, `job_results`, `job_progress`).

* **Postgres Backend: `register_worker` Fix**

    * The `queues` column (a `text[]` type) now correctly receives a one-element Python list (`[queue]`) instead of a bare string.
    * This resolves `DataError: invalid input for query argument $2`.

* **General Pause–Check Safety**

    * `process_job` now guards against backends that don't implement `is_queue_paused` by checking with `hasattr`, avoiding `AttributeError` on simple in-memory or dummy backends.

### Fixed

* Fixed Redis hash-scan code to handle both `bytes` and `str` keys/values, preventing `.decode()` errors.
* Ensured Postgres connection pool is wired into both `list_queues` and all worker-heartbeat methods.
* Cleaned up duplicate fixtures in test modules to prevent event-loop and fixture-resolution errors.
* Worker registration heartbeat tests were previously timing out, now pass reliably thanks to the configurable heartbeat interval.

## 0.1.0

Welcome to the **first official release** of **AsyncMQ**!

---

### 🚀 Highlights

* 🎉 A **100% asyncio & AnyIO** foundation—no more thread hacks or callback nightmares.
* 🔌 A **pluggable backend** system: Redis, Postgres, MongoDB, In-Memory, or your own.
* ⏱️ Robust **delayed** and **repeatable** job scheduling, including cron expressions.
* 🔄 Built-in **retries**, **exponential backoff**, and **time-to-live** (TTL) semantics.
* 💀 **Dead Letter Queues** (DLQ) for failed-job inspection and replay.
* ⚡ **Rate limiting** and **concurrency control** to protect downstream systems.
* 🐚 **Sandboxed execution** with subprocess isolation and fallback options.
* 📊 **Event Pub/Sub** hooks for `job:started`, `completed`, `failed`, `progress`, `cancelled`, and `expired`.
* 🔀 **Flow/DAG orchestration** via `FlowProducer`, with atomic and fallback dependency wiring.
* 🛠️ A **powerful CLI** for managing queues, jobs, and workers—JSON output for scripting.

---

### ✨ New Features

#### Core APIs

* **`@task` decorator**

  * Define sync or async functions as tasks.
  * Attach `.enqueue()` (alias `.delay()`) for one-line scheduling.
  * Support for **`retries`**, **`ttl`**, **`progress`** flag, **`depends_on`**, and **`repeat_every`**.

* **`Queue` class**

  * `add()`, `add_bulk()`, `add_repeatable()` for single, batch, and periodic jobs.
  * `pause()`, `resume()`, `clean()`, `queue_stats()`, and in-depth inspection.
  * Configurable **`concurrency`**, **`rate_limit`**, **`rate_interval`**, and **`scan_interval`**.

* **`process_job` & `handle_job`**

  * End-to-end lifecycle: dequeue, pause detection, TTL, delayed re-enqueue, execution (sandbox or direct), retry logic, DLQ, and events.

* **`run_worker` orchestrator**

  * Combines **ConcurrencyLimiter**, **RateLimiter**, **delayed_job_scanner**, and **repeatable_scheduler** into a single async entrypoint.

* **`repeatable_scheduler`**

  * Dynamic cron and interval scheduling with smart sleep intervals and high accuracy.
  * Utility `compute_next_run()` for dashboards and testing.

* **`FlowProducer`**

  * Enqueue entire job graphs/DAGs with dependencies, using atomic backend calls or safe fallback.

* **`Job` abstraction**

  * Rich state machine (WAITING, ACTIVE, COMPLETED, FAILED, DELAYED, EXPIRED).
  * Serialization via `to_dict()`/`from_dict()`, TTL checks, custom backoff strategies, dependencies, and repeat metadata.

#### Observability & Configuration

* **Settings** dataclass

  * Centralized configuration (`debug`, `logging_level`, `backend`, DB URLs, TTL, concurrency, rate limits, sandbox options, scan intervals).
  * Environment variable **`ASYNCMQ_SETTINGS_MODULE`** for overrides.

* **LoggingConfig** protocol

  * Built-in **`StandardLoggingConfig`** with timestamped console logs.
  * Pluggable for JSON, file rotation, or third-party handlers via custom `LoggingConfig` implementations.

* **EventEmitter** hooks for real-time job events, ideal for Prometheus metrics or Slack alerts.

#### Developer Experience

* **CLI**

  * `asyncmq queue`, `asyncmq job`, `asyncmq worker`, `asyncmq info` groups with intuitive commands and flags.
  * JSON and human-readable outputs, piping-friendly for shell scripts.

* **Documentation**

  * Comprehensive **Learn** section with deep-dive guides on every component.
  * **Features** reference for quick lookups.
  * **Performance Tuning** and **Security & Compliance** guides (coming soon).

* **Testing Utilities**

  * **InMemoryBackend** for fast, isolated unit tests.
  * Helpers for simulating delays, retries, failures, and cancellations.

---

### 🔄 Breaking Changes

* This is the **initial 0.1.0** release. There are no breaking changes yet! 🎉

---

### 🎯 Roadmap & Next Steps

* **Dashboard UI**: real-time job monitoring and management interface.
* **Plugin Ecosystem**: community-driven extensions for metrics, retries, and custom stores.

---

Thank you for choosing AsyncMQ! We can't wait to see what you build.

Happy tasking! 🚀
