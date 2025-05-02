# Core Concepts & Architecture

This deep dive explains exactly how AsyncMQ moves your tasks from producer to worker, how jobs are stored, retried, and expired, and why each design decision matters. Code references correspond to modules in the AsyncMQ source.

---

## 1. Queue Abstraction (`asyncmq.queues.Queue`)

A `Queue` object ties together the queue **name**, **backend storage**, **concurrency**, and **rate limit** settings.

* Provide a name and backend (The backend is optional as it will default to the `settings.backend`).
* Optionally tune concurrency (defaults to 3) and rate limit (disabled by default).

```python
{!> ../docs_src/concepts/queue.py !}
```

* **Why `.concurrency=3` by default?** Balances throughput without overwhelming your event loop or external resources.
Override for CPU-bound or I/O-heavy tasks.
* **Rate limiting** uses a token-bucket (`asyncmq.rate_limiter.RateLimiter`) to throttle job starts, ideal for API rate caps.

### 1.1. Enqueuing Jobs (delay)

* Create a Job instance via the task decorator (see Tasks section).
* Get a `payload` dict via `job.to_dict()`.

```python
await queue.enqueue(payload)                  # immediate
await queue.enqueue_delayed(payload, run_at)  # schedule for future
```

Under the hood, `enqueue()` delegates to `BaseBackend.enqueue`:

* **Redis**: `LPUSH queue:default:jobs <payload>`
* **Memory**: Python `list.insert(0, payload)`

Delayed jobs are stored separately (`Sorted Set` in Redis) and polled by `delayed_job_scanner`.

#### 1.1.1 Alternatively using the `delay`.

For those familiar to a different interface like Celery, this is the equivalent of `delay` which is great so AsyncMQ also
provides the `.delay()` option for you. It does literally the same as the `enqueue` combined with `enqueue_delayed`.

```python
await queue.delay(payload)                  # immediate
await queue.delay(payload, run_at)          # schedule for future
```

Under the hood, `delay()` delegates to `.enqueue(...)` and if a `run_at` is provided, then delegates to `.enqueue_delayed(...)`:

### 1.2. Inspecting & Controlling

```python
from asyncmq.core.enums import State

await queue.list_jobs(state=State.WAITING)
await queue.pause()   # workers stop dequeuing new jobs
await queue.resume()  # workers resume
stats = await queue.queue_stats()
```

* **pause()/resume()** flip a backend flag (`pause_queue`/`resume_queue`) preventing `dequeue()` from returning jobs.
* **queue_stats()** aggregates counts of jobs per state.

### 1.3. Running Workers via Code

AsyncIO-native:

```python
# Asynchronous entry point
await queue.run()
# or
queue.start()  # blocking, calls async run() internally via anyio.run
```

This uses `asyncmq.runners.run_worker`, passing your `queue.concurrency`, `rate_limit`, and any repeatable jobs
registered on this `Queue` instance.

* **Why `queue.start()`?** Provides a simple blocking call in synchronous scripts.

---

## 2. Task Registration (`asyncmq.tasks.task` decorator)

The `@task` decorator wraps your function to:

1. **Register** metadata in `TASK_REGISTRY` (`module.func_name`, queue, retries, TTL, progress).
2. **Attach** an `enqueue(backend, *args, **kwargs)` helper to schedule background execution.
3. **Wrap** execution logic so workers can call your function uniformly.

```python
{!> ../docs_src/concepts/register_task.py !}
```

* **Why explicit `.enqueue()`?** Separates function invocation from scheduling, preventing accidental background jobs.
* **Retries & TTL** managed per job: if your task raises, worker decrements retries and re-enqueues or moves to DLQ.

### 2.1. Without specifying a backend

You don't need to specify a `backend` if you already have one declared in the `settings.backend` which by default is `redis`.

```python
{!> ../docs_src/concepts/register_task_no_backend.py !}
```

---

## 3. Workers & Job Processing

### 3.1. Worker Components

Workers consist of three concurrent coroutines (via `asyncio.gather` in `run_worker`):

1. **`process_job`**: dequeues and handles jobs, respecting concurrency via semaphore and rate limits.
2. **`delayed_job_scanner`**: polls delayed storage (every `interval`, default 2s) to re-enqueue due jobs.
3. **`repeatable_scheduler`** (optional): if you’ve added repeatable jobs via `Queue.add_repeatable` or `FlowProducer`.

### 3.2. Job Lifecycle in `process_job` (`asyncmq.workers.process_job`)

1. **Dequeue**: `job_data = await backend.dequeue(queue_name)`
2. **Instantiate**: `job = Job.from_dict(job_data)`
3. **Cancellation Check**: `is_job_cancelled`; ack and emit `job:cancelled`
4. **TTL Check**: `job.is_expired()` → update state to `expired`, `move_to_dlq`, emit `job:expired`
5. **Dependency Check**: if `job.depends_on` unresolved, re-enqueue to wait
6. **Emit Event**: `job:started`
7. **Execute**: call function from `TASK_REGISTRY` with `await` or thread-wrap for sync functions
8. **Success**:
    * emit `job:completed`
    * `save_job_result`
    * if `repeat_every` set, schedule next run
9. **Failure**:
    * emit `job:failed`
    * if `retries_left > 0`, await `backend.enqueue(...)`
    * else, move to DLQ via `backend.move_to_dlq`
10. **Ack**: remove from active queue via `backend.ack`

!!! Note
    **Why separate `handle_job` and `process_job`?**

    * `process_job` manages loop, concurrency, and rate-limiting.
    * `handle_job` focuses on per-job lifecycle (TTL, retries, events).

### 3.3. CLI Worker vs. `Queue.run()`

* **CLI** (`asyncmq worker start default --concurrency 2`): uses `start_worker` in `asyncmq.runners`—spawns `worker_loop`s
(single coroutine per worker) without rate limiting or delayed scanner by default.
* **`queue.run()`**: uses `run_worker` - Integrates concurrency, rate limiting, delayed scanner, and repeatable scheduling.

!!! Tip
    Use CLI for quick testing; use `queue.run()` in long-running services to leverage full feature set.

---

## 4. Backends & Persistence

AsyncMQ decouples queue operations (`BaseBackend`) from low-level storage (`BaseStore` in `asyncmq.stores`).

| Backend             | Cargo                        | Data Structures                  | Use Case              |
| ------------------- | ---------------------------- | -------------------------------- | --------------------- |
| **InMemoryBackend** | tests & prototypes           | Python dicts, lists, heapq       | Fast, ephemeral       |
| **RedisBackend**    | `redis.py`                   | Lists (waiting), ZSets (delayed) | High throughput       |
| **PostgresBackend** | `postgres.py` uses `asyncpg` | Tables with indexes              | ACID, complex queries |
| **MongoBackend**    | `mongodb.py` using `motor`   | Collections with TTL indices     | Flexible schemas      |

The **default** backend used by `AsyncMQ` is redis and the reason for that its that AsyncMQ was heavily inspired by
**BullMQ which only works with Redis** but AsyncMQ is a lot more flexible allowing to support more than just that and allows
you to extend to your own store and backend if required.

!!! Note
    In the end, we just :heart: BullMQ so much that we wanted something like that in Python :rocket:.

### 4.1. Implementing Custom Backends

Subclass `asyncmq.backends.base.BaseBackend` and implement all abstract methods: enqueue, dequeue, ack, move_to_dlq, enqueue_delayed, get_due_delayed, remove_delayed, update_job_state, save_job_result, get_job_state, add_dependencies, resolve_dependency, pause_queue, resume_queue, and the repeatable methods.

> **Why this separation?** Enables adapting AsyncMQ to cloud queues, message brokers, or other persistence layers without touching core logic.

---

## 5. Scheduling & Flows

Beyond individual tasks and delays, AsyncMQ lets you orchestrate multiple, interdependent jobs as a flow using FlowProducer.
This is ideal for batch pipelines, DAGs, or any scenario where jobs must run in a specific order.

```python
{!> ../docs_src/concepts/flow.py !}
```

* **Atomic Flow**: Backends like PostgresBackend can enqueue all jobs and link dependencies in a single transaction
via atomic_add_flow (if implemented).
* **Fallback Logic**: For other backends, AsyncMQ sequentially calls enqueue for each job and then registers dependencies
with add_dependencies.
* **Repeatable Jobs**: To schedule a job to re-run automatically, set the repeat_every field on a Job (in seconds)
when constructing it. Workers will re-enqueue these on successful completion.

---

## 6. Event System (`asyncmq.core.event.event_emitter`)

AsyncMQ emits structured events at key points:

* `job:started`
* `job:progress` (when `progress=True`)
* `job:completed`
* `job:failed`
* `job:expired`
* `job:cancelled`

Subscribe to events to integrate metrics, logging, or side-effects:

```python
{!> ../docs_src/concepts/event_emmiter.py !}
```
Events use `anyio.EventStream` for async-safe pub/sub.

---

That concludes a precise, code-verified exploration of AsyncMQ’s core.
