# AsyncMQ

<p align="center">
  <a href="https://asyncmq.dymmond.com"><img src="https://res.cloudinary.com/dymmond/image/upload/v1746002620/asyncmq/oq2qhgqdlra7rudxaqhl.png" alt='AsyncMQ'></a>
</p>

<p align="center">
    <span>⚡ Supercharge your async applications with tasks so fast, you’ll think you’re bending time itself. ⚡</span>
</p>

<p align="center">
<a href="https://github.com/dymmond/asyncmq/actions/workflows/test-suite.yml/badge.svg?event=push&branch=main" target="_blank">
    <img src="https://github.com/dymmond/asyncmq/actions/workflows/test-suite.yml/badge.svg?event=push&branch=main" alt="Test Suite">
</a>

<a href="https://pypi.org/project/asyncmq" target="_blank">
    <img src="https://img.shields.io/pypi/v/asyncmq?color=%2334D058&label=pypi%20package" alt="Package version">
</a>

<a href="https://pypi.org/project/asyncmq" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/asyncmq.svg?color=%2334D058" alt="Supported Python versions">
</a>
</p>

---

Welcome to **AsyncMQ**—a modern, async-native task queue for Python that brings blazing speed and flexibility to your background processing.

## What Is AsyncMQ?

AsyncMQ is an **async-native**, **highly extensible** task queue built for Python (>=3.10). Drawing inspiration from giants like BullMQ and RQ, AsyncMQ is designed specifically for the speed demons of the asyncio era.

Key highlights:

* **Asynchronous First**: Built from the ground up on `asyncio` and `anyio` for native async/await support.
* **Pluggable Backends**: Choose from In-Memory, Redis, MongoDB, or Postgres backends (and add your own).
* **Automatic Retries & TTLs**: Jobs can be retried on failure and automatically expire based on time-to-live settings.
* **Dead Letter Queue**: Failed jobs get sent to a dead-letter queue for inspection and replay.
* **Delayed Jobs**: Schedule tasks to run in the future, with precise delay semantics.
* **Event Pub/Sub**: Subscribe to real-time job events and hook into logging, metrics, or notifications.
* **CLI Goodness**: Inspect, list, retry, and manage jobs straight from your terminal.
* **Esmerald, FastAPI or any ASGI Framework Ready**: Seamless integration with popular async web frameworks.

## Why You’ll Love It

* **Speed**: AsyncIO under the hood means minimal overhead and maximal throughput.
* **Flexibility**: Swap in any storage backend or hook into job events without changing your application code.
* **Simplicity**: Clean, consistent API that's easy to pick up—your first task can be queued in three lines of code.

## Prerequisites & Compatibility

* **Python**: 3.10+ (async features are best leveraged on 3.11+).
* **Backends**:

  * **In-Memory** (for testing or ephemeral tasks)
  * **Redis** (battle-tested, high-performance)
  * **MongoDB** (NoSQL storage)
  * **Postgres** (ACID guarantees)
  * **Custom**: Implement `BaseBackend` & `BaseStore` to add your own.

* **Frameworks**:

  * **Esmerald**, **FastAPI**, or any ASGI app.

## Core Components Overview

```text
+----------------+       +----------------+       +-------------+
|   Producer     |  -->  |     Queue      |  -->  |   Backend   |
+----------------+       +----------------+       +-------------+
                              ^   |
                              |   v
                          +--------+
                          | Worker |
                          +--------+
```

* **Producer**: Enqueues jobs onto named queues.
* **Queue**: High-level API for adding, scheduling, and inspecting jobs.
* **Backend**: Storage engine managing job persistence and state transitions.
* **Worker**: Processes jobs concurrently, with graceful shutdown and rate limiting.
