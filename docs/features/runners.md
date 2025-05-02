# Runners & Worker Loops

Runners are the heartbeats of AsyncMQ workers—they keep the show running by fetching jobs, handling them, and scheduling
delayed or repeatable tasks. 

This guide breaks down:

1. **`worker_loop`**: The basic job consumer.
2. **`start_worker`**: Spin up multiple loops with graceful shutdown.
3. **`run_worker`**: The all-in-one runner with concurrency, rate limiting, delayed scans, and repeatables.
4. **Under the Hood**: How these pieces fit together.
5. **Advanced Patterns & Tuning**
6. **Testing & Best Practices**
7. **Pitfalls & FAQs**

---

## 1. `worker_loop` — Your Basic Job Consumer

```python
async def worker_loop(
    queue_name: str,
    worker_id: int,
    backend: BaseBackend | None = None
):
    ...
```

* **Purpose**: Continuously fetches a single job, processes it, and loops.
* **`dequeue()`**: Blocks until a job is available or returns `None` quickly depending on backend.
* **`handle_job`**: Manages lifecycle (TTL, retries, events, execution).
* **Sleep**: Prevents CPU spin when no jobs are pending. Adjust for low-latency vs. CPU usage.

!!! Fun fact
    Without that `sleep`, your worker would be like an eager toddler—always running, never resting. 🛌

---

## 2. `start_worker` — Fan Out Multiple Loops

```python
async def start_worker(
    queue_name: str,
    concurrency: int = 1,
    backend: BaseBackend | None = None
):
    ...
```

* **`concurrency`**: Number of parallel consumers—higher for I/O-bound, lower for CPU-bound.
* **Graceful Shutdown**: Catches `CancelledError`, cancels all loops, and awaits them to finish.
* **Use Case**: CLI entrypoint `asyncmq worker start`; simple and robust.

!!! Tip
    Label your `worker_id` in logs to trace which loop did what when debugging.

---

## 3. `run_worker` — The Feature-Rich Orchestrator

```python
async def run_worker(
    queue_name: str,
    backend: BaseBackend | None = None,
    concurrency: int = 3,
    rate_limit: int | None = None,
    rate_interval: float = 1.0,
    repeatables: list[Any] | None = None,
    scan_interval: float | None = None,
) -> None:
    ...
```

### Key Components:

| Component                  | Role                                                                                |
| -------------------------- | ----------------------------------------------------------------------------------- |
| **Semaphore**              | Ensures only `concurrency` tasks run in parallel.                                   |
| **RateLimiter**            | Token-bucket to cap job starts per time window.                                     |
| **`process_job`**          | Dequeues and handles a job, managing TTL, retries, events, and execution.           |
| **`delayed_job_scanner`**  | Periodically moves due delayed jobs into the active queue.                          |
| **`repeatable_scheduler`** | Enqueues jobs based on `every` or `cron` definitions provided via `add_repeatable`. |

!!! Tip
    Tune `scan_interval` to be shorter than your smallest repeatable interval or delay for sub-second scheduling precision.

---

## 4. Under the Hood

1. **`worker_loop`**: Base consumer, straightforward but no bells and whistles.
2. **`start_worker`**: Fan-out `worker_loop` with shutdown handling.
3. **`run_worker`**: Replaces `start_worker` when you need:

   * Concurrency control
   * Rate limiting
   * Delayed & repeatable job scheduling
4. **`process_job` vs. `handle_job`**: `process_job` manages concurrency & rate limit; `handle_job` manages job lifecycle.

---

## 5. Advanced Patterns & Tuning

### 5.1. Dynamic Concurrency

Adjust `concurrency` at runtime by canceling and re-launching `run_worker` with a new value—useful when load spikes.

### 5.2. Burst Rate Limiting

Combine `rate_limit` with `rate_interval` to allow bursts followed by cool-down. Great for API rate caps.

### 5.3. Custom Scanners

Write your own scanner (e.g., for external triggers) by replicating the pattern in `delayed_job_scanner` and adding it to `tasks` in `run_worker`.

---

## 6. Testing & Best Practices

* **Unit Test `worker_loop`**: Use `InMemoryBackend`; seed jobs; run a few iterations; assert calls to `handle_job`.
* **Simulate Shutdown**: Cancel `start_worker` tasks and assert cleanup via `task.cancelled()`.
* **Load Testing `run_worker`**: Combine with benchmarks to measure throughput and latency.

!!! Tip
    Use `pytest-asyncio` with timeouts to prevent hanging tests when loops never terminate.

---

## 7. Pitfalls & FAQs

* **Busy Waiting**: Too small `sleep` in `worker_loop` increases CPU usage; too large adds latency.
* **Zero `rate_limit`**: Blocks all processing—this is by design. Never default to `rate_limit=0` unless you truly want a pause.
* **Missing `scan_interval`**: Default from settings may be too slow for your cron jobs—override per-queue or globally.
* **Graceful Shutdown**: Always cancel `run_worker` or `start_worker` tasks, not the event loop, to allow cleanup.

---

With these runners demystified, you can architect robust, scalable worker processes, complete with rate limits, retries, and precision scheduling.
