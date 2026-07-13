"""Parameterized AsyncMQ load runner for repeatable local benchmark runs."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from typing import Any

import anyio

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.core.event import event_emitter
from asyncmq.jobs import Job
from asyncmq.tasks import TASK_REGISTRY
from asyncmq.workers import process_job

TASK_ID = "benchmarks.load_asyncmq.payload_task"


@dataclass(frozen=True)
class LoadResult:
    jobs: int
    workers: int
    concurrency: int
    payload_bytes: int
    enqueue_latency_ns: int
    total_latency_ns: int
    throughput_jobs_per_second: float
    completed: int
    failed: int


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
    completed = 0
    failed = 0
    finished = anyio.Event()
    counter_lock = anyio.Lock()

    async def _count_finished(status: str, data: dict[str, Any]) -> None:
        nonlocal completed, failed
        if data.get("task_id", data.get("task")) != TASK_ID:
            return
        async with counter_lock:
            if status == "completed":
                completed += 1
            else:
                failed += 1
            if completed + failed >= jobs:
                finished.set()

    async def on_completed(data: dict[str, Any]) -> None:
        await _count_finished("completed", data)

    async def on_failed(data: dict[str, Any]) -> None:
        await _count_finished("failed", data)

    enqueue_start = time.perf_counter_ns()
    for index in range(jobs):
        job = Job(task_id=TASK_ID, args=[payload], kwargs={}, job_id=f"load-{index}")
        await backend.enqueue(queue, job.to_dict())
    enqueue_latency_ns = time.perf_counter_ns() - enqueue_start

    event_emitter.on("job:completed", on_completed)
    event_emitter.on("job:failed", on_failed)
    total_start = time.perf_counter_ns()
    try:
        async with anyio.create_task_group() as tg:
            for _ in range(workers):
                tg.start_soon(process_job, queue, anyio.CapacityLimiter(concurrency), None, backend)

            with anyio.fail_after(timeout):
                await finished.wait()
            tg.cancel_scope.cancel()
    finally:
        event_emitter.off("job:completed", on_completed)
        event_emitter.off("job:failed", on_failed)

    total_latency_ns = time.perf_counter_ns() - total_start
    throughput = completed / (total_latency_ns / 1_000_000_000) if total_latency_ns else 0.0

    return LoadResult(
        jobs=jobs,
        workers=workers,
        concurrency=concurrency,
        payload_bytes=payload_bytes,
        enqueue_latency_ns=enqueue_latency_ns,
        total_latency_ns=total_latency_ns,
        throughput_jobs_per_second=throughput,
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
