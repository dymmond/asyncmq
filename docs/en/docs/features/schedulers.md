# Schedulers

AsyncMQ has two scheduling loops:

- delayed job scanner (`asyncmq.core.delayed_scanner.delayed_job_scanner`)
- repeatable scheduler (`asyncmq.schedulers.repeatable_scheduler`)

## Delayed Job Scanner

`delayed_job_scanner(queue_name, backend=None, interval=2.0)`:

- pops due delayed jobs from backend
- re-enqueues them into the main queue
- sleeps `interval`

`run_worker(...)` starts this loop automatically.

## Repeatable Scheduler

`repeatable_scheduler(queue_name, jobs, backend=None, interval=None)`:

- consumes local repeatable definitions passed in `jobs`
- also polls backend-managed repeatables registered through `Queue.upsert_repeatable(...)`
- supports `every` and `cron`
- enqueues due jobs continuously and advances backend-managed schedules after each emission

AsyncMQ now supports two repeatable registration modes:

- local repeatables via `Queue.add_repeatable(...)`
- durable repeatables via `Queue.upsert_repeatable(...)`

Use local repeatables when the worker process itself is the source of truth for
the schedule. Use durable repeatables when the schedule should survive producer
restarts, dashboard-created schedules, or operational tooling.

Example:

```python
from asyncmq.queues import Queue

queue = Queue("emails")

# Durable schedule stored in the backend.
await queue.upsert_repeatable(
    "myapp.tasks.send_digest",
    cron="0 7 * * *",
    kwargs={"tenant": "acme"},
    retries=3,
)

# Local schedule only visible to this worker process.
queue.add_repeatable("myapp.tasks.cleanup", every=60)
```

## `compute_next_run(job_def)`

Utility for next execution time:

- cron definition -> next cron timestamp
- `every` -> `now + every`

## Tuning

Use `scan_interval` (global settings or queue override) to tune delayed/repeatable polling cadence.
Lower intervals reduce scheduling latency at the cost of more backend polling.
