---
hide:
  - navigation
---

# Release Notes

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
