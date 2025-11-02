# Queues & `Queue` API

Queues are the traffic directors of AsyncMQ, orchestrating the flow of jobs from producers to workers.
This guide covers everything about the `Queue` class—its constructor knobs, core methods, advanced patterns,
and best practices—served with a dash of humor to keep you awake. 🚦

---

## 1. What Is `Queue`?

The `Queue` class (in `asyncmq.queues`) is your main interface to:

* **Schedule** jobs (single, bulk, delayed, repeatable)
* **Inspect** queue state (waiting, active, completed, failed)
* **Control** worker behavior (pause, resume, run, start)
* **Clean** up old or unwanted jobs

Behind the scenes, `Queue` delegates to a `BaseBackend` for persistence and uses `run_worker` to launch workers.

!!! Metaphor
    Think of `Queue` as a circus ringmaster—directing acts (jobs), ensuring no clowns (failures) escape,
    and cueing the next performer on time. 🎪

---

## 2. Constructor Parameters

```python
{!> ../../../docs_src/queues/queue.py !}
```

| Param           | Type                  | Default                  | Purpose & Tuning Tips                                                                        |
| --------------- | --------------------- | ------------------------ | -------------------------------------------------------------------------------------------- |
| `name`          | `str`                 | —                        | Identifier for this queue. Keep it short and meaningful (e.g., "email", "reports").          |
| `backend`       | `BaseBackend` or None | `settings.backend`       | Storage driver (Redis, Postgres, etc.). Override for custom or test backends.                |
| `concurrency`   | `int`                 | `3`                      | Max simultaneous jobs. High for I/O-bound, low for CPU-bound.                                |
| `rate_limit`    | `int` or None         | `None`                   | Token-bucket max calls per `rate_interval`. Block with `0`, disable with `None`.             |
| `rate_interval` | `float`               | `1.0`                    | Window (seconds) for rate limiting.                                                          |
| `scan_interval` | `float` or None       | `settings.scan_interval` | Polling frequency for delayed & repeatable jobs. Lower = lower latency but more backend ops. |

> 🔧 **Pro Tip:** Set `scan_interval` shorter than your shortest delay or cron window to avoid missing schedules.

---

## 3. Core Methods

### 3.1. `add(...)` (Single Job)

Schedules a single job:

```python
{!> ../../../docs_src/queues/job.py !}
```

* **Returns** `job_id` (UUID).
* **`delay`** (sec): schedule future execution.
* **`priority`**: lower numbers run first.
* **`retries`** & **`ttl`**: control failure and expiry.

### 3.2. `add_bulk([...])` (Batch Enqueue)

Efficiently enqueue many jobs:

```python
ids = await queue.add_bulk([
    {"task_id": "app.process", "args": [1]},
    {"task_id": "app.process", "args": [2]},
])
```

* **Use case**: high-throughput ingestion or batch jobs.
* **Backend support**: must implement `bulk_enqueue`.

### 3.3. `add_repeatable(...)` (Periodic Jobs)

Register repeatable definitions:

```python
{!> ../../../docs_src/queues/repeatable.py !}
```

* **`every`**: interval in seconds or a cron-like string.
* **`cron`**: cron expression (requires repeatable scheduler).
* **Note**: Jobs enqueued when `run()` starts.

### 3.4. `pause()` / `resume()`

Temporarily halt or resume workers:

```python
await queue.pause()
# ... maintenance window ...
await queue.resume()
```

* Prevents new dequeues; in-flight jobs finish.

### 3.5. `clean(state, older_than)` (Purge)

Purge jobs by state & age:

```python
await queue.clean("completed", older_than=time.time() - 3600)
```

* **`state`**: e.g., "completed", "failed", "expired".
* **`older_than`**: Unix timestamp threshold.

---

## 4. Running Workers

### 4.1. `run()` (Async)

Starts the worker with:

* Concurrency & rate limiting
* Delayed-job scanner (using `scan_interval`)
* Repeatable scheduler (for `_repeatables`)

```python
# In an async context
await queue.run()
```

### 4.2. `start()` (Blocking)

Convenience wrapper using AnyIO:

```bash
# In a script
queue.start()
```

* Blocks until cancellation (SIGINT) or error.

!!! Tip
    Combine with ASGI lifespan events to manage workers in web apps.

---

## 5. Inspecting & Utilities

* **`queue.enqueue(payload)`** / **`enqueue_delayed`**: low-level methods.
* **`queue.delay(payload, run_at)`**: alias for enqueue + enqueue_delayed.
* **`queue.get_due_delayed()`**, **`list_delayed()`**, **`remove_delayed(job_id)`**: manage delayed set.
* **`list_repeatables()`**, **`pause_repeatable()`**, **`resume_repeatable()`**: introspect repeatables.
* **`cancel_job(job_id)`**, **`is_job_cancelled(job_id)`**: cancel or check cancellations.
* **`queue_stats()`**: counts per state.
* **`list_jobs(state)`**: list jobs by state.

```python
stats = await queue.queue_stats()
jobs = await queue.list_jobs("waiting")
```

---

## 6. Advanced Patterns & Examples

### 6.1. Dependency Chains

```python
# Step A
id_a = await queue.add("app.step_a")
# Step B after A
await queue.add("app.step_b", depends_on=[id_a])
```

### 6.2. Parallel Fan-Out

```python
# A triggers two parallel tasks, then aggregates
id_a = await queue.add("app.ingest")
await queue.add_bulk([
    {"task_id": "app.transform_a", "depends_on": [id_a]},
    {"task_id": "app.transform_b", "depends_on": [id_a]},
])
# aggregator waits on both
await queue.add("app.aggregate", depends_on=[id_a, id_b])
```

### 6.3. Cron via `FlowProducer`

For full cron support, see the **Advanced Patterns** guide. You can also register a repeatable with `cron` in `add_repeatable`.

---

## 7. Testing & Best Practices

* **Use InMemoryBackend** for unit tests:

  ```python
  from asyncmq.backends.inmemory import InMemoryBackend
  q = Queue(name="test", backend=InMemoryBackend())
  ```
* **Simulate Delays**: set `delay=0.1` and `await anyio.sleep(0.2)`.
* **Assert Stats**: `stats = await q.queue_stats()`.

!!! Warning
    Don't call `queue.run()` in tests without a timeout—tests will hang!

---

## 8. Common Pitfalls & FAQs

* **Missing Backend**: If you omit `backend`, uses global `settings.backend`—watch for surprises if you override per-queue.
* **Zero `rate_limit`**: Blocks all processing (by design)—use carefully.
* **Repeatable Jobs Not Firing**: Ensure you call `run()` (or `start()`) so the scheduler runs.
* **Cleaning Too Aggressively**: Purging mid-run can orphan dependencies—schedule cleanups during off-hours.

---

Armed with the `Queue` API and these examples, you can tame any workload from simple enqueues to complex
DAG orchestrations—while having a laugh or two along the way. 🎉
