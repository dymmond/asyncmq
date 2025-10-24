---
hide:
  - navigation
---

# Release Notes

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

    * Brand new [Dashboard](./dashboard/index.md) where it shows how to integrate with your AsyncMQ. This dahboard is still
in *beta* mode so any feedback is welcomed.

* **Top-Level CLI Commands**
  Added four new top-level commands to the `asyncmq` CLI (powered by Click & Rich):

    * `list-queues` — list all queues known to the backend
    * `list-workers` — show all registered workers with queue, concurrency, and last heartbeat
    * `register-worker <worker_id> <queue> [--concurrency N]` — register or bump a worker’s heartbeat **and** concurrency
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

    * `process_job` now guards against backends that don’t implement `is_queue_paused` by checking with `hasattr`, avoiding `AttributeError` on simple in-memory or dummy backends.

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

Thank you for choosing AsyncMQ! We can’t wait to see what you build.

Happy tasking! 🚀
