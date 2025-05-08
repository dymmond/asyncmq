# AsyncMQ

<p align="center">
  <a href="https://asyncmq.dymmond.com"><img src="https://res.cloudinary.com/dymmond/image/upload/v1746002620/asyncmq/oq2qhgqdlra7rudxaqhl.png" alt="AsyncMQ Logo"></a>
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
  <a href="https://img.shields.io/pypi/pyversions/asyncmq.svg?color=%2334D058" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/asyncmq.svg?color=%2334D058" alt="Supported Python versions">
  </a>
</p>

---

Welcome to **AsyncMQ**, the modern task queue that brings **powerful**, **flexible**, and **lightning-fast** background processing to your Python stack. Whether you're building microservices, handling high-throughput data pipelines, or just want reliable delayed jobs, AsyncMQ has your back.

## 🚀 Why Choose AsyncMQ?

1. **Asyncio & AnyIO-Native**
   No more wrestling with threads or callbacks—AsyncMQ is built on `asyncio` and `anyio`, so your tasks are non-blocking and integrate seamlessly into modern async frameworks like FastAPI and Esmerald.

2. **Multi-Backend Flexibility**
   From the blistering speed of Redis to the ACID guarantees of Postgres, or the schema-less power of MongoDB, AsyncMQ supports multiple backends out of the box—and you can write your own by implementing `BaseBackend`.

3. **Advanced Scheduling**

   * **Delayed Jobs**: Fire off tasks in the future with precise delays.
   * **Repeatable & Cron**: Create heartbeat jobs or cron-like schedules with a single line.
   * **Scan & Scheduler**: Smart polling intervals and cron logic keep your timings accurate without burning CPU cycles.

4. **Robust Error Handling**

   * **Retries & Backoff**: Exponential or custom backoff strategies to gracefully handle failures.
   * **Dead Letter Queue (DLQ)**: Automatically route permanently failed tasks for later inspection and reprocessing.

5. **Observability & Event Hooks**
   Tap into lifecycle events (`job:started`, `job:completed`, `job:failed`, `job:progress`) to power real-time dashboards, metrics, or custom alerts.

6. **Rate Limiting & Concurrency Control**
   Fine-tune throughput with token-bucket rate limiting and capacity limiters. Scale your worker concurrency without overloading downstream systems.

7. **Sandboxed Execution**
   Run untrusted or CPU-bound tasks in isolated subprocesses with timeouts to protect your main workers from rogue code or infinite loops.

8. **Flow/DAG Orchestration**
   Create complex task graphs with dependencies using `FlowProducer`. Enqueue entire pipelines atomically or fall back to safe sequential logic.

9. **CLI & Dev Experience**
   A feature-rich CLI for managing queues, jobs, and workers—intuitive commands, JSON output support, and built-in help for every scenario.

10. **Seamless ASGI Integration**
    Out-of-the-box compatibility with FastAPI, Esmerald, or any ASGI application. Manage workers within your app’s lifecycle events.

---

## Installation

AsyncMQ requires Python 3.10+ to run and that its because the EOL will be later on only.

You can install in many different ways, the default brings `redis` to use with `RedisBackend`.

```shell
$ pip install asyncmq
```

**Postgres**

If you prefer the Postgres backend.

```shell
$ pip install asyncmq[postgres]
```

**Mongo DB**

You might want also Mongo DB backend.

```shell
$ pip install asyncmq[mongo]
```

**All in one go**

You might want to install everything and see what is the best for you.

```shell
$ pip install asyncmq[all]
```

### Dashboard

AsyncMQ comes with a dashboard and that requires some additional setup but nothing special but to install it.

```shell
$ pip install asyncmq[dashboard]
```

---

## Comparison with Other Python Task Queues

| Feature                    | **AsyncMQ**                      | Celery        | RQ         | Dramatiq     | Huey       |
| -------------------------- | -------------------------------- | ------------- | ---------- | ------------ | ---------- |
| **AsyncIO Native**         | ✅                                | ❌             | ❌          | ✅            | ❌          |
| **Pluggable Backends**     | ✅ Redis, Postgres, Mongo, In-Mem | RabbitMQ only | Redis only | Redis only   | Redis only |
| **Rate Limiting**          | ✅ built-in                       | ❌             | ❌          | ✅ via addon  | ❌          |
| **Cron & Repeatable Jobs** | ✅ interval & cron expressions    | ✅             | ✅          | ✅            | ✅          |
| **Progress Reporting**     | ✅ event hooks                    | ✅ callbacks   | ❌          | ✅ hooks      | ❌          |
| **Dead Letter Queue**      | ✅                                | ✅             | ❌          | ✅            | ✅          |
| **Flow / DAG Support**     | ✅ `FlowProducer`                 | ✅ chords      | ❌          | ✅ extensions | ❌          |
| **ASGI-Friendly**          | ✅ FastAPI/Esmerald integration   | ❌             | ❌          | ❌            | ❌          |
| **CLI Management**         | ✅ rich, JSON-friendly            | ✅             | ✅          | ✅            | ✅          |

---

### Where **AsyncMQ** Shines

* **True AsyncIO & AnyIO integration**: zero thread hacks, full non-blocking background tasks.
* **Backend flexibility**: swap Redis, Postgres, MongoDB, or In-Memory with a single setting.
* **Built-in rate limiting & concurrency control**: protect downstream services out-of-the-box.
* **Event-driven hooks**: subscribe to `job:started`, `completed`, `failed`, `progress`, etc., for metrics and alerts.
* **ASGI integration**: manage workers within FastAPI or Esmerald lifecycles—no extra wrappers needed.

---

## ⚡ Core Features at a Glance

| Category               | Highlights                                                                                |
| ---------------------- | ----------------------------------------------------------------------------------------- |
| **Task Definition**    | `@task` decorator, `.enqueue()`, progress callbacks, TTL, retries, dependencies, repeats  |
| **Queue API**          | `add()`, `add_bulk()`, `add_repeatable()`, `pause()`, `resume()`, `clean()`, stats        |
| **Worker Engine**      | `process_job()`, capacity & rate limiters, sandbox, event emitter, DLQ handling           |
| **Scheduling**         | `delayed_job_scanner`, `repeatable_scheduler`, cron vs interval support                   |
| **Flow Orchestration** | `FlowProducer.add_flow()`, atomic backend support, fallback mode for dependency wiring    |
| **Configuration**      | Central `Settings` dataclass, env var override, dynamic tuning of concurrency & intervals |
| **Observability**      | LoggingConfig protocol, structured logs, event subscriptions, metrics integration         |
| **CLI Management**     | `asyncmq queue`, `asyncmq job`, `asyncmq worker`, `asyncmq info` commands                 |

---

## 🎬 Quickstart

1. **Install**

   ```bash
   pip install asyncmq
   ```
2. **Configure**

   Override the default settings `from asyncmq.conf import monkay` and create your own, then make it global.

   ```bash
   export ASYNCMQ_SETTINGS_MODULE=myapp.settings.Settings
   ```
3. **Define a Task**

   ```python
   from asyncmq.tasks import task

   @task(queue="emails", retries=2, ttl=120)
   async def send_welcome(email: str):
       # Imagine real email-sending logic here
       print(f"📧 Sent welcome to {email}")
   ```
4. **Enqueue & Run**

   ```python
   import anyio
   from asyncmq.queues import Queue

   async def main():
       q = Queue("emails")
       await send_welcome.enqueue(q.backend, "alice@example.com", delay=10)
   anyio.run(main)
   ```
5. **Start Workers**

   ```bash
   asyncmq worker start emails --concurrency 5
   ```

---

AsyncMQ is more than just a task queue, it’s the swiss army knife of async background processing.
Dive into the **Learn** section to master every feature, or jump into the **Features** docs for quick reference.

Ready to bend time?

**Get started today** and experience async tasking at warp speed! 🎉
