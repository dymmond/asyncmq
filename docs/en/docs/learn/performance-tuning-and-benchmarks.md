# Performance Tuning

Tune AsyncMQ using workload-driven benchmarks, not static defaults.

## Benchmark Commands

AsyncMQ ships two benchmark entrypoints:

```shell
hatch run benchmark_plan
hatch run benchmark
hatch run benchmark_load
```

`benchmark_plan` prints the canonical workload dimensions, measurement policy,
Python/runtime metadata, and whether Celery, Dramatiq, Arq, RQ, and Huey are
installed in the current environment. It is intentionally standard-library only
so it can run before optional competitor dependencies are installed.

`benchmark` runs the repository's CodSpeed benchmark suite. Today that suite
covers job object creation/serialization and the `InMemoryBackend` operation
path. Treat those numbers as AsyncMQ core/runtime microbenchmarks, not as
networked broker or competitive production throughput results.

`benchmark_load` runs a parameterized AsyncMQ in-memory load benchmark and
prints JSON with enqueue latency, total latency, throughput, worker count,
concurrency, payload size, CPU time, max RSS, completed jobs, and failed jobs.
Use it for local runtime smoke measurements before moving to real backend and
competitor runs:

```shell
hatch run python -m benchmarks.load_asyncmq \
  --jobs 10000 \
  --workers 100 \
  --concurrency 1 \
  --payload-bytes 128 \
  --json
```

For competitive runs, install the external queue libraries and brokers in the
same environment, then require availability before recording results:

```shell
hatch run benchmark_competitors
```

If a competitor is missing, the command exits non-zero rather than silently
producing an incomplete comparison.

## Benchmark Methodology

Use the benchmark plan as the source of truth for comparable workloads:

| Workload | Jobs | Payload | Workers | Queue depth | Purpose |
| --- | ---: | ---: | ---: | ---: | --- |
| `small-payload` | 10,000 | 128 B | 1 | 10,000 | enqueue/dequeue latency and steady throughput |
| `large-payload` | 2,000 | 256 KiB | 1 | 2,000 | serialization, broker bandwidth, and memory pressure |
| `worker-fanout-100` | 100,000 | 128 B | 100 | 100,000 | first production-scale horizontal worker target |
| `worker-fanout-1000` | 1,000,000 | 128 B | 1,000 | 1,000,000 | autoscaling and broker contention target |

Record at least five repetitions after warmup and report median, P95, P99, min,
and max. Use `time.perf_counter_ns` for local timing, and record CPU, RSS,
queue depth, and recovery time alongside latency and throughput. Keep broker
versions, Python version, worker concurrency, connection pool sizes, durability
settings, and machine shape attached to every published result.

Do not compare AsyncMQ with Celery, Dramatiq, Arq, RQ, or Huey unless all
competitor libraries and required brokers were present in the measured
environment and the same workload dimensions were used for every system.
In-memory load results are useful for runtime regression detection, but they are
not a substitute for broker-backed measurements.

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
