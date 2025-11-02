# Features

Welcome to the **Features** section of the AsyncMQ documentation! Here you'll find detailed guides on each core
capability of AsyncMQ. Click through to learn how to wield each feature like a pro:

* [Settings & Configuration](./settings.md)
  Centralized, type-safe, and environment‑driven configuration for backends, concurrency, rate limits, sandboxing, TTL, and more.

* [Logging](./logging.md)
  Pluggable logging setup via `LoggingConfig`, with a default console implementation and examples for custom JSON or file‑based handlers.

* [Tasks & `@task` Decorator](./tasks.md)
  Define async or sync functions as background jobs, enqueue them with ease, report progress, chain dependencies, and schedule repeats.

* [Queues & `Queue` API](./queues.md)
  High‑level interface to enqueue jobs (single, bulk, delayed, repeatable), pause/resume processing, inspect state, clean up old jobs, and start workers.

* [Workers & Processing Loops](./workers.md)
  The heart of AsyncMQ: concurrency and rate limiting, job lifecycle management (`process_job` and `handle_job`), events, retries, and the `Worker` helper.

* [Runners & Worker Entrypoints](./runners.md)
  Core runner functions (`worker_loop`, `start_worker`, and `run_worker`) that tie together concurrency, rate limiting, delayed scans, and repeatable scheduling.

* [Schedulers & Cron Utilities](./schedulers.md)
  Metronomes for your jobs: the `repeatable_scheduler` loop, dynamic sleep logic, `compute_next_run`, and trade‑offs between cron expressions and fixed intervals.

* [Sandboxed Execution](./sandbox.md)
  Isolate untrusted or heavy tasks in subprocesses, enforce timeouts, capture errors safely, and optionally fall back to inline execution.

* [Jobs & `Job` Class](./jobs.md)
  The atomic unit of work: states, TTL checks, retry/backoff strategies, serialization (`to_dict`/`from_dict`), dependencies, and repeatable metadata.

* [Flows & `FlowProducer`](./flows.md)
  Orchestrate multi‑step pipelines or DAGs: atomic enqueue of job graphs with dependencies, fallback logic, and backend‑optimized operations.

* [CLI Reference](./cli.md)
  Your terminal command center for queue and job inspection, worker startup, pausing/resuming queues, retrying or cancelling jobs, and viewing backend info.

* [Performance Tuning & Benchmarking](../learn/performance-tuning-and-benchmarks.md)
  In‑depth strategies and examples for measuring throughput, tuning concurrency and rate limits, optimizing scan intervals, and backend‑specific best practices.

* [Security & Compliance](../learn/security-and-compliance.md)
  Lock down your pipelines with authentication, encryption, ACLs, audit logging, and compliance guidance (GDPR, HIPAA, PCI DSS).
---

Navigate to any feature above to dive deep—each guide is packed with examples, best practices, troubleshooting tips,
and a dash of humor to keep you smiling as you build robust, scalable background job systems with AsyncMQ.
