# Performance Tuning

Tune AsyncMQ using workload-driven benchmarks, not static defaults.

## Benchmark Commands

AsyncMQ ships two benchmark entrypoints:

```shell
hatch run benchmark_plan
hatch run benchmark
hatch run benchmark_load
hatch run benchmark_prepare
hatch run benchmark_compare
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
prints JSON with warmup/repetition counts, per-sample measurements, median,
P95, P99, min, and max statistics, enqueue latency, total latency, throughput,
worker count, per-worker concurrency, total concurrency, payload size, CPU
time, max RSS, completed jobs, and failed jobs. Use it for local runtime smoke
measurements before moving to real backend and competitor runs:

```shell
hatch run python -m benchmarks.load_asyncmq \
  --jobs 10000 \
  --workers 100 \
  --concurrency 1 \
  --payload-bytes 128 \
  --warmup-jobs 1000 \
  --repetitions 5 \
  --json
```

For competitive runs, install the external queue libraries and brokers in the
same environment, then require availability before recording results:

```shell
hatch run benchmark_competitors
```

If a competitor is missing, the command exits non-zero rather than silently
producing an incomplete comparison.

For executable comparisons, use the Redis-backed competitive harness. It reuses
the benchmark plan and runner statistics, but isolates competitor dependencies
in per-target virtual environments because some queue libraries require
incompatible Redis client versions:

```shell
hatch run benchmark_prepare
hatch run python -m benchmarks.competitive \
  --targets asyncmq,celery,dramatiq,arq,rq,huey \
  --workload small-payload \
  --repetitions 5 \
  --json
```

Use an isolated Redis database or disposable Redis instance for competitive
runs. The harness flushes the selected Redis database before each sample and
records both completed jobs and processed payload bytes for every target using
the same single-command counter update.
Pass `--dry-run --workload <name>` before a large run to inspect the resolved
jobs, workers, concurrency, payload size, warmup, and target interpreters.
Pass `--no-progress-timeout <seconds>` to bound competitor samples that stop
advancing their completion counter before the overall sample timeout. The
sample `--timeout` also bounds external producer subprocesses during enqueue.
For 1,000-worker local runs, tune `ASYNCMQ_BENCH_COUNTER_MAX_CONNECTIONS` and
`ASYNCMQ_BENCH_COUNTER_POOL_TIMEOUT` if the benchmark counter itself becomes the
bottleneck.

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
For competitive runs, `total_latency_ns` and throughput are measured from
enqueue start until all jobs complete, with workers already running for every
target.

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

- Redis: monitor latency, memory, and key cardinality. Ready queues index job
  IDs, runnable jobs are claimed with one Redis-side transition, and
  state/result changes use compact metadata. Large `args`/`kwargs` fields are
  split from the Lua-readable metadata and compressed when beneficial.
- Postgres: monitor locks, table/index bloat, and connection pool saturation.
- MongoDB: monitor write/read latency and collection/index growth.
- RabbitMQ: monitor queue depth, unacked messages, and broker flow control.
