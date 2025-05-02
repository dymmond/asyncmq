# Release Notes

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
