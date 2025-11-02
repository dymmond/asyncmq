# Workers & Job Processing

Workers are the engine room of AsyncMQ—they breathe life into tasks by dequeuing jobs, enforcing limits, handling errors,
and emitting events at every stage. Buckle up for a deep dive into the `process_job` loop, rate limiting,
concurrency control, lifecycle management, and the `Worker` class helper.

---

## 1. The `process_job` Loop

```python
async def process_job(
    queue_name: str,
    limiter: CapacityLimiter,
    rate_limiter: RateLimiter | None = None,
    backend: BaseBackend | None = None,
) -> None:
    ...
```

* **Concurrency**: Controlled by a `CapacityLimiter` (semaphore-like).
* **Rate Limiting**: Optional `RateLimiter` (token-bucket) caps throughput.
* **Pause Support**: Respects `backend.is_queue_paused()`.
* **Task Group**: Spawns handlers asynchronously, never blocking the loop.

!!! Info "Fun fact"
    Without the small sleep on empty dequeue, this loop would spin like a fidget spinner—CPU-intensive and dizzying.

## 2. Applying Limits: `_run_with_limits`

* **`limiter`**: Only `N` jobs run in parallel.
* **`rate_limiter`**: If set, waits for token before each job.
* **Delegation**: Calls `handle_job` for full lifecycle management.


## 3. Lifecycle Management: `handle_job`

```python
async def handle_job(
    queue_name: str,
    raw_job: dict[str, Any],
    backend: BaseBackend | None = None,
) -> None:
    ...
```

### Key Points

1. **Cancellation**: Early exit if job cancelled, with `job:cancelled` event.
2. **Expiration (TTL)**: Jobs past TTL go to DLQ, `job:expired` event.
3. **Delay Handling**: Respects `delay_until`, requeues without firing.
4. **Events**: Emits `job:started`, `job:completed`, `job:failed`, `job:expired`, `job:cancelled`.
5. **Retries**: Honors `max_retries`, schedules retry with backoff via `next_retry_delay()`.
6. **Sandbox Integration**: Calls `sandbox.run_handler` if enabled, else direct/async call.

!!! Tip
    Listen to events via `event_emitter.on("job:failed", ...)` for alerting or custom fallback logic.


## 4. The `Worker` Helper Class

```python
class Worker:
    def __init__(self, queue: Queue) -> None: ...
    def start(self): ...
    def stop(self): ...
```

* **Purpose**: Encapsulates worker startup & graceful shutdown.
* **Automatic Task Discovery**: Before registering and processing any jobs, the worker runs `autodiscover_tasks()`
(which imports all modules in your custom [tasks declared in the settings](./settings.md) so your `@task(...)`
handlers are registered).
* **CancelScope**: Allows `.stop()` to cleanly cancel processing.
* **Defaults**: Uses `settings.worker_concurrency`, `settings.rate_limit`, and `settings.rate_interval`.

!!! Tip
    Use `Worker(q).start()` in scripts or container entrypoints, and `.stop()` on application shutdown.


## 5. Advanced Patterns & Tuning

* **Dynamic Scaling**: Change `settings.worker_concurrency` and restart workers for elastic scaling.
* **Burst Control**: Combine a high `concurrency` with a low `rate_limit` for bursts capped by API quotas.
* **Custom Events**: Subscribe to `job:*` events to integrate with tracing or metrics.


## 6. Testing & Best Practices

* **Unit Tests**: Swap `backend` to `InMemoryBackend` and simulate a handful of jobs; assert job states.
* **Simulate Failures**: Write a task that raises, check DLQ routing and retry counts.
* **Pause/Resume**: Test `is_queue_paused()` logic by toggling pause state in backend mocks.

!!! Tip
    Use `pytest-timeout` to guard against hanging loops during tests.


## 7. Common Pitfalls & FAQs

* **Silent Failures**: Missing `await` on `process_job` or `Worker.start()` results in no work.
* **Overly Aggressive Sleeping**: Long sleep on `None` dequeue inflates latency; adjust `0.1` value thoughtfully.
* **Event Flooding**: `job:progress` or `job:started` events can flood if not throttled—unsubscribe or filter in handlers.

## 8. Worker Lifecycle Hooks

AsyncMQ lets you define lifecycle hooks for workers, special functions that run automatically when a worker
starts up or shuts down.

They're ideal for tasks like connecting to databases, warming caches, or closing resources gracefully.

### Declaring Hooks

You can declare them directly in your AsyncMQ [settings](./settings.md) file:

```python
from asyncmq.protocols.lifespan import Lifespan

async def on_startup(backend, worker_id, queue):
    print(f"Worker {worker_id} started for queue: {queue}")
    # Example: await backend.connect_pool()

async def on_shutdown(backend, worker_id, queue):
    print(f"Worker {worker_id} shutting down for queue: {queue}")
    # Example: await backend.close_pool()

worker_on_startup = [on_startup]
worker_on_shutdown = [on_shutdown]
```

### Hook Rules

* Hooks can be sync or async. AsyncMQ detects and awaits them automatically.
* You can define a single function or a list/tuple of multiple hooks.
* Hooks receive the following context arguments:
    * backend: the active backend instance
    * worker_id: unique worker UUID
    * queue: queue name the worker is processing


### Example — Synchronous Hooks

```python
def warm_cache(backend, worker_id, queue):
    print("Warming cache for", queue)

def log_shutdown(backend, worker_id, queue):
    print("Flushing logs for", worker_id)

worker_on_startup = [warm_cache]
worker_on_shutdown = [log_shutdown]
```

### Reusable Hook Modules

For complex setups, it's a good practice to organize hooks in a reusable module (e.g.: `lifecycle.py` or whatever you want to call):

```python title="myproject/lifecycle.py"
async def connect_db(backend, worker_id, queue):
    await backend.connect_pool()

async def disconnect_db(backend, worker_id, queue):
    await backend.close_pool()

```

Then in your settings:

```
from myproject import lifecycle

worker_on_startup = [lifecycle.connect_db]
worker_on_shutdown = [lifecycle.disconnect_db]
```

### Behind the Scenes

Under the hood, AsyncMQ uses the same lifecycle engine across the platform:
    * run_hooks() — executes hooks during startup, propagates exceptions.
    * run_hooks_safely() — executes hooks during shutdown, swallows exceptions by default.

This ensures that startup failures are visible, while shutdown always cleans up gracefully.

By leveraging lifecycle hooks, you make your workers state-aware, resource-conscious, and self-cleaning.
A professional touch that turns simple background jobs into a resilient orchestration layer. 🚀

---

With the worker framework mastered, you can pipeline tens of thousands of jobs, handle errors gracefully,
and keep an observant eye on every lifecycle event all while maintaining a cheeky grin. 😎
