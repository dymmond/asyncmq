# Performance Tuning

Tune AsyncMQ using workload-driven benchmarks, not static defaults.

## Primary Levers

- `worker_concurrency` / `--concurrency`
- `rate_limit` + `rate_interval` on queues/runners
- `scan_interval` for delayed/repeatable loops
- backend-specific connection pooling options

## Concurrency Guidance

- I/O-heavy tasks: increase concurrency gradually.
- CPU-heavy tasks: keep concurrency lower or use sandbox/process isolation.

## Scan Interval Tradeoff

- Lower `scan_interval` => lower delayed-job latency, higher backend polling.
- Higher `scan_interval` => less backend load, higher scheduling latency.

Tune based on acceptable delay for delayed/repeatable jobs.

## Measure What Matters

Track at least:

- enqueue throughput
- completion throughput
- queue depth over time
- failure/retry rate
- P95/P99 task duration

## Backend Notes

- Redis: monitor latency, memory, and key cardinality.
- Postgres: monitor locks, table/index bloat, and connection pool saturation.
- MongoDB: monitor write/read latency and collection/index growth.
- RabbitMQ: monitor queue depth, unacked messages, and broker flow control.
