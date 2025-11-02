# Performance Tuning & Benchmarking

Welcome to the deep-dive where we squeeze every last drop of performance out of AsyncMQ because milliseconds matter,
and bragging rights at benchmark time are essential. 🏎️💨

In this chapter you'll learn how to:
1. Measure baseline performance with realistic workloads.
2. Tune worker concurrency and rate limits.
3. Optimize delayed-job scanning and cron scheduling.
4. Minimize backend latency (Redis, Postgres, MongoDB).
5. Integrate monitoring & profiling tools.
6. Interpret benchmarks and avoid common pitfalls.

---

## 1. Benchmarking Your Workload

Before you tune, you must measure! Use a representative workload:

```python
{!> ../../../docs_src/tutorial/bench/benchmarks.py !}
```

* **Metric Separation**: Track enqueue vs. processing durations independently.
* **Repeatable Tests**: Run multiple iterations and average to smooth out noise.

!!! Tip
    **Pro tip**: Isolate benchmarking on dedicated systems to avoid background load skewing results.

---

## 2. Concurrency & Rate Limits

### 2.1. Worker Concurrency

* **I/O-bound tasks**: scale up concurrency (10–50) for better throughput.
* **CPU-bound tasks**: keep concurrency low (1–4) to avoid thread contention.

Tune via:

```bash
asyncmq worker start perf --concurrency 20
```

Or programmatically:

```python
queue = Queue(name="perf", backend=backend, concurrency=20)
```

### 2.2. Rate Limiting

Throttle job starts using token bucket:

```python
queue = Queue(
    name="api_calls",
    backend=backend,
    rate_limit=5,         # max 5 jobs
    rate_interval=1.0     # per second
)
```

Ideal for external APIs with rate caps.

---

## 3. Scanning & Scheduling Overhead

### 3.1. Delayed Job Scanner

Tuning how often AsyncMQ checks for due delayed and repeatable jobs can dramatically influence both task latency and
backend load. You now have two configuration options:

1. **Global Setting** via your `Settings` class (applies to all queues):

   ```python
   {!> ../../../docs_src/tutorial/bench/settings.py !}
   ```

2. **Per-Queue Override** using the new `scan_interval` parameter on `Queue` (applies only to that instance):

   ```python
   {!> ../../../docs_src/tutorial/bench/per_queue.py !}
   ```

#### Latency vs. Scan Frequency Tradeoff

| scan_interval | Approx. Max Delay | Backend Calls/sec |
| -------------: | --------------- | ----------------- |
|           0.1s | ~0.1s           | 10× baseline      |
|           1.0s | ~1.0s           | 1× baseline       |
|           5.0s | up to 5.0s      | 0.2× baseline     |

* **Lower intervals**: near real-time but higher backend load.
* **Higher intervals**: lower load but increased scheduling latency.

Benchmark the delay between `delay_until` and actual execution to pick the sweet spot.

### 3.2. Cron & Repeatable Scheduling

High-frequency or cron-based jobs (e.g., every minute) rely on the same scanner. Ensure your scanner interval is shorter than your cron window:

```python
# Schedule a job every minute
queue.add_repeatable(
    task_id="app.cleanup",
    cron="*/1 * * * *",
)
```

With `scan_interval=0.2`, jobs will fire within \~200ms of each minute boundary.

---

## 4. Backend Latency Optimization

### 4.1. Redis

* **Pipelines**: batch `LPUSH` + metadata writes.
* **Connection Pool**: adjust `minsize`/`maxsize` to match concurrency.
* **Key Expiration**: TTL on job hashes to auto-clean old entries.

### 4.2. Postgres

* **Indexes**: on `status`, `run_at`, `priority`.
* **Prepared Statements**: reuse queries for enqueue & dequeue.
* **Pooling**: `asyncpg.create_pool(max_size=20)`.

### 4.3. MongoDB

* **TTL Index**: expire delayed & completed docs automatically.
* **Bulk Writes**: batch insert many jobs.

---

## 5. Monitoring & Profiling

* **Prometheus**: expose counters/histograms via `event_emitter`:

  ```python
  from prometheus_client import Counter, Histogram, start_http_server
  from asyncmq.core.event import event_emitter

  JOB_STARTED = Counter('asyncmq_job_started', 'Jobs started')
  JOB_DURATION = Histogram('asyncmq_job_duration_seconds', 'Task duration')

  event_emitter.on('job:started', lambda _: JOB_STARTED.inc())
  event_emitter.on('job:completed', lambda p: JOB_DURATION.observe(
      p['timestamps']['finished_at'] - p['timestamps']['created_at']
  ))

  start_http_server(8000)
  ```

* **Profilers**: use Py-Spy or cProfile to spot hotspots in worker loops.

---

## 6. Interpreting Results

1. **Enqueue vs Process**: Large gaps suggest backend or worker bottlenecks.
2. **95th/99th Percentiles**: flag outlier tasks.
3. **Queue Depth Trends**: sustained depth >0 indicates under-provisioned workers.
4. **Retry Rates**: high retry ratio points to flaky tasks or misconfigs.

> **Final tip:** Automate benchmarks in CI to catch regressions—no surprises in production! 🎉

---

With these strategies, you'll wield AsyncMQ like a power tool, fast, reliable, and scalable.

Next up: **Security & Compliance**—locking down your pipeline end-to-end.
