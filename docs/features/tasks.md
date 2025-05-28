# Tasks & `@task` Decorator

Tasks are the building blocks of AsyncMQ—mini-applications you write, decorate, and forget about, as they run asynchronously in the background. This guide is your one-stop reference for everything related to tasks: from basic usage to advanced patterns, complete with witty asides to keep you smiling while you read.

---

## 1. What Is a Task?

A **task** in AsyncMQ is simply a Python function (sync or async) that you mark with the `@task` decorator. Behind the scenes, this decorator:

1. **Registers** your function in the global `TASK_REGISTRY` with metadata (queue name, retries, TTL, progress flag).
2. **Attaches** an `enqueue()` helper (and alias `delay()`) so you can schedule jobs in one line.
3. **Wraps** your function so it plays nicely with both `asyncio` and threads, and emits progress events if requested.

> 🎩 **Magician’s Note:** Decorating doesn’t run your function—only `.enqueue()` does. No accidental background jobs!

---

## 2. Basic Usage

### 2.1. Defining and Registering a Task

```python
{!> ../docs_src/tasks/task.py !}
```

* **`queue`**: Logical grouping—workers listening to the "default" queue will pick this up.
* **`retries`**: How many times to retry on exception (default 0).
* **`ttl`**: If job sits unprocessed longer than 60s, it moves to the DLQ.
* **`progress`** (optional): Injects a `report_progress` callback so you can stream progress updates.

### 2.2. Enqueuing Jobs

```python
from asyncmq.logging import logger

job_id = await send_email.enqueue(
    "alice@example.com",
    "Welcome!",
    "Thanks for joining.",
    backend=backend,       # Optional: AsyncMQSettings.backend if omitted
    delay=5.0,             # run 5 seconds from now
    priority=3,            # higher urgency
    depends_on=[prev_job_id],  # wait for another job
    repeat_every=None       # no repeats
)
logger.info(f"Scheduled email job: {job_id}")
```

* **`backend`** parameter defaults to global settings, no need to pass it everywhere.
* **`delay`**, **`priority`**, **`depends_on`**, and **`repeat_every`** cover most scheduling needs.
* The helper returns the **`job_id`**, so you can track or cancel it later.

---

## 3. Advanced Features

### 3.1. Progress Reporting

To stream progress from long-running tasks:

```python
{!> ../docs_src/tasks/task_simple.py !}
```

Workers will emit `job:progress` events via the `event_emitter`, which you can subscribe to:

```python
{!> ../docs_src/tasks/event.py !}
```

### 3.2. Dependency Chaining

Chain jobs so that one runs after another:

```python
{!> ../docs_src/tasks/chain.py !}
```

Under the hood, tasks with `depends_on` call `backend.add_dependencies()` before enqueue.

### 3.3. Repeatable / Cron-Like Tasks

Auto-reenqueue tasks at intervals:

```python
{!> ../docs_src/tasks/requeue.py !}
```

Or use `cron` (via `FlowProducer` for DAGs)—see the advanced patterns guide.

---

## 4. Under the Hood

1. **Decorator**: Creates `enqueue_task()` and `wrapper()` functions.
2. **`TASK_REGISTRY`**: Maps `task_id` → metadata (function, queue, retries, ttl, progress).
3. **`wrapper()`**:
    * Injects `report_progress` if needed.
    * Calls original `func`, awaiting async or using `anyio.to_thread` for sync functions.
4. **`enqueue_task()`**:
    * Constructs a `Job` with all metadata.
    * Optionally calls `add_dependencies()`.
    * Enqueues immediately or via `enqueue_delayed()`.

Peek at all registered tasks:

```python
from asyncmq.tasks import list_tasks
from asyncmq.logging import logger

logger.info(list_tasks())
```

---

## 5. Testing & Best Practices

* **Unit Tests**: Monkeypatch `backend` to `InMemoryBackend()` for fast, isolated tests.
* **Edge Cases**: Test TTL expiration by setting `ttl=0.1` and sleeping past it.
* **Error Handling**: Simulate exceptions in functions and assert retries via `Job.max_retries`.
* **Naming**: Use clear, module-qualified IDs (e.g., `reports.generate_report`) to avoid collisions.

!!! Tip
    Combine `pytest`’s `monkeypatch` and AsyncMQ’s `InMemoryBackend` to simulate delays, failures, and concurrency
    without spinning up Redis or Postgres.

---

## 6. Common Pitfalls & FAQs

* **Accidental Direct Calls**: Calling `send_email("x")` runs the function immediately.
Always use `send_email.enqueue(...)` for background jobs.
* **Missing `ASYNCMQ_SETTINGS_MODULE`**: If you forget to set it, tasks will default to `RedisBackend()`
surprise if you intended Postgres!
* **Progress without Workers**: Enqueueing with `progress=True` alone doesn’t stream updates unless workers invoke the event emitter.
* **Overlapping Retries**: If your retry backoff is zero, failing jobs can spam your backend. Add `backoff`
(future feature) or custom delays.

---

## 7. Glossary & Quick Reference

| Term                | Definition                                                           |
| ------------------- | -------------------------------------------------------------------- |
| **`task_id`**       | Unique string `<module>.<func>` identifying the task.                |
| **`enqueue()`**     | Schedules a job; does not execute immediately.                       |
| **`wrapper()`**     | The function workers call to run your task with proper context.      |
| **`Job`**           | Immutable snapshot of task arguments + metadata enqueued in a queue. |
| **`TASK_REGISTRY`** | Global dict mapping `task_id` to metadata for all decorated tasks.   |

---

That’s a wrap on tasks! Now you can write, schedule, and monitor AsyncMQ tasks like a seasoned pro—with a grin 😁.
