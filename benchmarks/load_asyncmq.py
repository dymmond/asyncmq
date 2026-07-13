"""Parameterized AsyncMQ load runner for repeatable local benchmark runs."""

from __future__ import annotations

import argparse
import json
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
    payload_bytes: int
    enqueue_latency_ns: int
    total_latency_ns: int
    throughput_jobs_per_second: float
    cpu_user_seconds: float | None
    cpu_system_seconds: float | None
    max_rss_kb: int | None
    completed: int
    failed: int


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


async def run_load(
    *,
    jobs: int,
    workers: int,
    concurrency: int,
    payload_bytes: int,
    timeout: float,
) -> LoadResult:
    if jobs <= 0:
        raise ValueError("jobs must be greater than zero")
    if workers <= 0:
        raise ValueError("workers must be greater than zero")
    if concurrency <= 0:
        raise ValueError("concurrency must be greater than zero")
    if payload_bytes < 0:
        raise ValueError("payload_bytes must be greater than or equal to zero")

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

    return LoadResult(
        jobs=jobs,
        workers=workers,
        concurrency=concurrency,
        payload_bytes=payload_bytes,
        enqueue_latency_ns=enqueue_latency_ns,
        total_latency_ns=total_latency_ns,
        throughput_jobs_per_second=throughput,
        cpu_user_seconds=cpu_user_seconds,
        cpu_system_seconds=cpu_system_seconds,
        max_rss_kb=max_rss_kb,
        completed=completed,
        failed=failed,
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
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args(argv)

    async def _run() -> LoadResult:
        return await run_load(
            jobs=args.jobs,
            workers=args.workers,
            concurrency=args.concurrency,
            payload_bytes=args.payload_bytes,
            timeout=args.timeout,
        )

    result = anyio.run(_run)
    if args.json:
        print(json.dumps(asdict(result), indent=2, sort_keys=True))
    else:
        _print_human(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
