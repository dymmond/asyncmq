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

- consumes repeatable definitions passed in `jobs`
- supports `every` and `cron`
- enqueues due jobs continuously

Important: this scheduler operates on in-memory definitions provided at worker startup (for example from `Queue.add_repeatable`).

## `compute_next_run(job_def)`

Utility for next execution time:

- cron definition -> next cron timestamp
- `every` -> `now + every`

## Tuning

Use `scan_interval` (global settings or queue override) to tune delayed/repeatable polling cadence.
Lower intervals reduce scheduling latency at the cost of more backend polling.
