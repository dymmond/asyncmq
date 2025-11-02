# Schedulers & Cron Utilities

Schedulers are the metronomes of AsyncMQ, ensuring your repeatable jobs fire on time—whether it's every few seconds or according to a cron expression. In this guide, we'll explore:

1. **`repeatable_scheduler`**: the async loop that reads your definitions and enqueues due jobs.
2. **`compute_next_run`**: a handy utility for calculating the next timestamp.
3. **Cron vs. Interval**: trade-offs and pitfalls.
4. **Integration**: how to wire the scheduler into your worker processes.
5. **Testing & Best Practices**
6. **Common Pitfalls & FAQs**

---

## 1. What Is `repeatable_scheduler`?

Defined in `asyncmq.core.delayed_scanner`, this coroutine:

* **Loads** a list of job definitions, each with either a `cron` or `every` key.
* **Maintains** state: next cron timestamps and last-run markers.
* **Loops** indefinitely, waking up at dynamic intervals to enqueue ready jobs.
* **Enqueues** each due job via the configured backend's `enqueue()` method.

Signature:

```python
async def repeatable_scheduler(
    queue_name: str,
    jobs: list[dict[str, Any]],
    backend: BaseBackend | None = None,
    interval: float | None = None,
) -> None: ...
```

* **`queue_name`**: target queue for scheduled jobs.
* **`jobs`**: definitions from `Queue._repeatables`:

  ```python
  {
    "task_id": "app.cleanup",
    "every": 3600,         # run hourly
    "cron": "0 0 * * *", # or cron expression
    "args": [...],
    "kwargs": {...},
    "retries": 1, "ttl": 60, "priority": 5
  }
  ```
* **`backend`**: overrides `settings.backend` if provided.
* **`interval`**: minimum sleep between loops (default 30s).

### 1.1. How It Works

1. **Initialization**
    * Create `croniter` instances for all `cron` jobs, storing next-run times.
    * Set a `check_interval` (either `interval` or default 30s).
2. **Scheduler Loop**
    * **Dynamic sleep** ensures you don't miss a cron boundary by sleeping too long.
    * **Minimum sleep** of 0.1s prevents busy loops.

!!! Tip
    For sub-second accuracy on high-frequency repeatables, set `interval` (or your queue's `scan_interval`) to a small
    value, but beware of increased backend load.

---

## 2. `compute_next_run` Utility

A pure function to calculate the next run timestamp, useful for testing, dashboards, or custom schedulers:

```python
def compute_next_run(job_def: dict[str, Any]) -> float: ...
```

* **Cron**: uses `croniter` to honor complex schedules (e.g., `"*/5 * * * *"`).
* **Every**: simple fixed delay.

!!! Use Case:
    Pre-calculate next runs for monitoring UIs or load testing your scheduler logic.

---

## 3. Cron vs. Interval

| Feature              | `every` Interval                        | `cron` Expression                  |
| -------------------- | --------------------------------------- | ---------------------------------- |
| Syntax               | Numeric seconds                         | Standard cron (minute, hour, etc.) |
| Flexibility          | Fixed-period only                       | Complex schedules (e.g., weekdays) |
| Next-run Computation | Simple addition                         | Library-powered parsing            |
| Drift                | Prone to drift if processing > interval | Anchored to wall-clock, less drift |

* **Drift Management**: Cron avoids cumulative drift; intervals may drift if job scheduling takes longer than the interval itself.
* **Complexity**: Cron has a learning curve; intervals are trivial.

---

## 4. Integration with Workers

`repeatable_scheduler` is wired into `run_worker`:

* **Delayed Scanner** handles once-off `delay` jobs.
* **Repeatable Scheduler** handles cyclic jobs.

> 🚀 **Pro Tip:** Keep your repeatable definitions idempotent—if a job accidentally enqueues twice (e.g., at exact boundary), it shouldn't cause chaos.

---

## 5. Testing & Best Practices

* **Unit Test `compute_next_run`**:

  ```python
  import time
  job = {"every": 10}
  next1 = compute_next_run(job)
  assert next1 - time.time() == pytest.approx(10, abs=0.1)
  job_cron = {"cron": "0 * * * *"}
  next_cron = compute_next_run(job_cron)
  assert isinstance(next_cron, float)
  ```

* **Test Loop Behavior**: Provide an `interval=0.1`, a short list of jobs with both `every` and `cron`, and patch `time.time()` to simulate multiple cycles.
* **Resource Usage**: Beware CPU if you set `interval` too low; monitor scheduler task CPU.

---

## 6. Common Pitfalls & FAQs

* **Missing Keys**: Definitions must include either `cron` or `every`—or they silently never run.
* **Time Zones**: `croniter` uses system local time; if your app crosses DST boundaries, cron triggers adjust accordingly.
* **Long-Running Jobs**: If your scheduled job takes longer than its interval, you may see overlap—consider guarding with job-level locks.
* **Scheduler Crash**: Exceptions inside the scheduler loop can kill it—wrap body in `try/except` or let run_worker's gather propagate cancellation.

**FAQ:** *What if I need a one-off job at a specific timestamp?*
Use `queue.add(..., delay=timestamp - time.time())` or leverage your backend's `enqueue_delayed` directly.

---

With schedulers and cron utilities in your toolkit, you can orchestrate everything from simple heartbeats to complex
ETL pipelines on schedule, keeping your systems in perfect rhythm! 🥁
