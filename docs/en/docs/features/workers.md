# Workers

AsyncMQ worker execution is centered on `process_job()` and `handle_job()` in `asyncmq.workers`.

## `process_job()`

`process_job(queue_name, limiter, rate_limiter=None, backend=None)`:

- loops forever
- checks `is_queue_paused(queue_name)`
- dequeues jobs
- spawns `_run_with_limits(...)` for concurrency + optional rate limiting

## `handle_job()` Lifecycle

For each job, worker logic performs:

1. normalize backend payload shape (supports wrapped payloads)
2. dependency gate check (`depends_on` parent states)
3. cancellation check
4. TTL expiration handling (`expired` -> DLQ)
5. delayed execution handling (`delay_until`)
6. set `active` state and emit `job:started`
7. run task (sandboxed or direct)
8. success path:
- set `completed`
- save result
- ack
- resolve dependencies
- emit `job:completed`
9. failure path:
- increment retries
- retry with backoff via delayed enqueue, or
- mark `failed`, ack, move to DLQ, emit `job:failed`

## Worker Wrapper Class

`Worker(queue, heartbeat_interval=None)`:

- registers worker metadata/heartbeats via backend
- optionally autodiscovers tasks from `settings.tasks`
- runs lifecycle hooks:
- `worker_on_startup`
- `worker_on_shutdown`
- runs `process_job(...)` until cancelled

Entrypoints:

- `worker.start()` blocking
- `await worker.run()` async
- `worker.stop()` cooperative cancel when running via `start()`

## Lifecycle Hooks

Hooks can be sync or async callables. They receive keyword arguments (`backend`, `worker_id`, `queue`).

```python
class AppSettings(Settings):
    worker_on_startup = [my_async_startup_hook]
    worker_on_shutdown = [my_sync_shutdown_hook]
```

## Related

- [Runners](runners.md)
- [Jobs](jobs.md)
- [Stalled Recovery](../troubleshooting.md#stalled-jobs-not-recovered)
