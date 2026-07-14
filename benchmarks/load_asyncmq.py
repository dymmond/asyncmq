"""Parameterized AsyncMQ load runner for repeatable local benchmark runs."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
from dataclasses import asdict, dataclass

import anyio

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.jobs import Job
from asyncmq.tasks import TASK_REGISTRY
from asyncmq.workers import process_job

TASK_ID = "benchmarks.load_asyncmq.payload_task"

try:
    import resource
except ImportError:  # pragma: no cover - resource is Unix-only.
    resource = None  # type: ignore[assignment]


@dataclass(frozen=True)
class LoadResult:
    jobs: int
    workers: int
    concurrency: int
    total_concurrency: int
    payload_bytes: int
    warmup_jobs: int
    repetitions: int
    enqueue_latency_ns: int
    total_latency_ns: int
    throughput_jobs_per_second: float
    cpu_user_seconds: float | None
    cpu_system_seconds: float | None
    max_rss_kb: int | None
    completed: int
    failed: int
    samples: list[dict[str, int | float | None]]
    statistics: dict[str, dict[str, int | float]]


def _resource_snapshot() -> tuple[float | None, float | None, int | None]:
    if resource is None:
        return None, None, None

    usage = resource.getrusage(resource.RUSAGE_SELF)
    max_rss = int(usage.ru_maxrss)
    if sys.platform == "darwin":
        max_rss = max_rss // 1024
    return float(usage.ru_utime), float(usage.ru_stime), max_rss


async def _payload_task(payload: str) -> int:
    return len(payload)


def _percentile(values: list[int | float], percentile: float) -> int | float:
    if not values:
        raise ValueError("values must not be empty")
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil((percentile / 100) * len(ordered)) - 1))
    return ordered[index]


def _metric_statistics(samples: list[dict[str, int | float | None]]) -> dict[str, dict[str, int | float]]:
    metrics = (
        "enqueue_latency_ns",
        "total_latency_ns",
        "throughput_jobs_per_second",
        "worker_startup_ns",
        "cpu_user_seconds",
        "cpu_system_seconds",
        "max_rss_kb",
    )
    result: dict[str, dict[str, int | float]] = {}
    for metric in metrics:
        values = [sample[metric] for sample in samples if sample.get(metric) is not None]
        if not values:
            continue
        numeric_values = [value for value in values if isinstance(value, int | float)]
        result[metric] = {
            "median": statistics.median(numeric_values),
            "p95": _percentile(numeric_values, 95),
            "p99": _percentile(numeric_values, 99),
            "min": min(numeric_values),
            "max": max(numeric_values),
        }
    return result


async def _run_load_sample(
    *,
    jobs: int,
    workers: int,
    concurrency: int,
    payload_bytes: int,
    timeout: float,
) -> dict[str, int | float | None]:
    backend = InMemoryBackend()
    queue = "benchmark-load"
    payload = "x" * payload_bytes
    TASK_REGISTRY[TASK_ID] = {"func": _payload_task}

    async def terminal_counts() -> tuple[int, int]:
        completed_count = len(backend.job_results)
        failed_count = len(backend.dlqs.get(queue, []))
        return completed_count, failed_count

    cpu_user_start, cpu_system_start, _ = _resource_snapshot()
    enqueue_start = time.perf_counter_ns()
    for index in range(jobs):
        job = Job(task_id=TASK_ID, args=[payload], kwargs={}, job_id=f"load-{index}")
        await backend.enqueue(queue, job.to_dict())
    enqueue_latency_ns = time.perf_counter_ns() - enqueue_start

    total_start = time.perf_counter_ns()
    async with anyio.create_task_group() as tg:
        for _ in range(workers):
            tg.start_soon(process_job, queue, anyio.CapacityLimiter(concurrency), None, backend)

        with anyio.fail_after(timeout):
            while True:
                completed, failed = await terminal_counts()
                if completed + failed >= jobs:
                    break
                await anyio.sleep(0.01)
        tg.cancel_scope.cancel()

    total_latency_ns = time.perf_counter_ns() - total_start
    completed, failed = await terminal_counts()
    throughput = completed / (total_latency_ns / 1_000_000_000) if total_latency_ns else 0.0
    cpu_user_end, cpu_system_end, max_rss_kb = _resource_snapshot()
    cpu_user_seconds = None
    cpu_system_seconds = None
    if cpu_user_start is not None and cpu_user_end is not None:
        cpu_user_seconds = cpu_user_end - cpu_user_start
    if cpu_system_start is not None and cpu_system_end is not None:
        cpu_system_seconds = cpu_system_end - cpu_system_start

    return {
        "enqueue_latency_ns": enqueue_latency_ns,
        "total_latency_ns": total_latency_ns,
        "throughput_jobs_per_second": throughput,
        "cpu_user_seconds": cpu_user_seconds,
        "cpu_system_seconds": cpu_system_seconds,
        "max_rss_kb": max_rss_kb,
        "completed": completed,
        "failed": failed,
    }


async def run_load(
    *,
    jobs: int,
    workers: int,
    concurrency: int,
    payload_bytes: int,
    timeout: float,
    warmup_jobs: int = 0,
    repetitions: int = 1,
) -> LoadResult:
    if jobs <= 0:
        raise ValueError("jobs must be greater than zero")
    if workers <= 0:
        raise ValueError("workers must be greater than zero")
    if concurrency <= 0:
        raise ValueError("concurrency must be greater than zero")
    if payload_bytes < 0:
        raise ValueError("payload_bytes must be greater than or equal to zero")
    if warmup_jobs < 0:
        raise ValueError("warmup_jobs must be greater than or equal to zero")
    if repetitions <= 0:
        raise ValueError("repetitions must be greater than zero")

    TASK_REGISTRY[TASK_ID] = {"func": _payload_task}

    if warmup_jobs:
        await _run_load_sample(
            jobs=warmup_jobs,
            workers=workers,
            concurrency=concurrency,
            payload_bytes=payload_bytes,
            timeout=timeout,
        )

    samples = [
        await _run_load_sample(
            jobs=jobs,
            workers=workers,
            concurrency=concurrency,
            payload_bytes=payload_bytes,
            timeout=timeout,
        )
        for _ in range(repetitions)
    ]
    stats = _metric_statistics(samples)

    return LoadResult(
        jobs=jobs,
        workers=workers,
        concurrency=concurrency,
        total_concurrency=workers * concurrency,
        payload_bytes=payload_bytes,
        warmup_jobs=warmup_jobs,
        repetitions=repetitions,
        enqueue_latency_ns=int(stats["enqueue_latency_ns"]["median"]),
        total_latency_ns=int(stats["total_latency_ns"]["median"]),
        throughput_jobs_per_second=float(stats["throughput_jobs_per_second"]["median"]),
        cpu_user_seconds=(float(stats["cpu_user_seconds"]["median"]) if "cpu_user_seconds" in stats else None),
        cpu_system_seconds=(float(stats["cpu_system_seconds"]["median"]) if "cpu_system_seconds" in stats else None),
        max_rss_kb=int(stats["max_rss_kb"]["max"]) if "max_rss_kb" in stats else None,
        completed=int(min(sample["completed"] or 0 for sample in samples)),
        failed=int(max(sample["failed"] or 0 for sample in samples)),
        samples=samples,
        statistics=stats,
    )


def _print_human(result: LoadResult) -> None:
    print("AsyncMQ load result")
    for key, value in asdict(result).items():
        print(f"{key}: {value}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run an AsyncMQ in-memory load benchmark.")
    parser.add_argument("--jobs", type=int, default=1_000)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--warmup-jobs", type=int, default=0)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args(argv)

    async def _run() -> LoadResult:
        return await run_load(
            jobs=args.jobs,
            workers=args.workers,
            concurrency=args.concurrency,
            payload_bytes=args.payload_bytes,
            timeout=args.timeout,
            warmup_jobs=args.warmup_jobs,
            repetitions=args.repetitions,
        )

    result = anyio.run(_run)
    if args.json:
        print(json.dumps(asdict(result), indent=2, sort_keys=True))
    else:
        _print_human(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
