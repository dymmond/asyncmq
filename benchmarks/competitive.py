"""Competitive benchmark harness for AsyncMQ and external task queues."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import venv
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import anyio

from asyncmq.backends.redis import RedisBackend
from asyncmq.jobs import Job
from asyncmq.tasks import TASK_REGISTRY
from asyncmq.workers import process_job
from benchmarks.load_asyncmq import _metric_statistics, _resource_snapshot
from benchmarks.plan import WORKLOADS

REDIS_URL = "redis://localhost:6379/15"
ASYNCMQ_TASK_ID = "benchmarks.competitive.asyncmq_payload_task"
_ASYNCMQ_COUNTERS: dict[str, Any] = {}
ASYNCMQ_COUNTER_MAX_CONNECTIONS = 2048
ASYNCMQ_COUNTER_POOL_TIMEOUT = 30.0


@dataclass(frozen=True)
class Target:
    name: str
    packages: tuple[str, ...]
    description: str


@dataclass(frozen=True)
class CompetitiveResult:
    target: str
    workload: str | None
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
    worker_startup_ns: int
    cpu_user_seconds: float | None
    cpu_system_seconds: float | None
    max_rss_kb: int | None
    completed: int
    failed: int
    timed_out: bool
    samples: list[dict[str, int | float | bool | None]]
    statistics: dict[str, dict[str, int | float]]


TARGETS: dict[str, Target] = {
    "asyncmq": Target("asyncmq", (), "AsyncMQ RedisBackend using this Hatch environment."),
    "celery": Target("celery", ("celery[redis]>=5.6,<6",), "Celery with Redis broker/result backend."),
    "dramatiq": Target("dramatiq", ("dramatiq[redis]>=2.2,<3",), "Dramatiq with RedisBroker."),
    "arq": Target("arq", ("arq>=0.28,<0.29",), "Arq with Redis queue workers."),
    "rq": Target("rq", ("rq>=2.10,<3",), "RQ with Redis workers."),
    "huey": Target("huey", ("huey>=3.2,<4", "redis>=5,<6"), "Huey with RedisHuey."),
}


def _completion_key(run_id: str) -> str:
    return f"asyncmq:benchmark:{run_id}:completed"


def _bytes_key(run_id: str) -> str:
    return f"asyncmq:benchmark:{run_id}:bytes"


def _venv_python(root: Path, target: str) -> Path:
    bindir = "Scripts" if sys.platform == "win32" else "bin"
    return root / target / bindir / "python"


def _target_env(redis_url: str, queue: str, concurrency: int) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "ASYNCMQ_BENCH_CONCURRENCY": str(concurrency),
            "ASYNCMQ_BENCH_QUEUE": queue,
            "ASYNCMQ_BENCH_REDIS_URL": redis_url,
            "PYTHONUNBUFFERED": "1",
        }
    )
    return env


def _run_subprocess(
    command: Sequence[str],
    *,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> None:
    subprocess.run(command, check=True, env=env, text=True, timeout=timeout)


def prepare_envs(targets: Sequence[str], root: Path) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    root.mkdir(parents=True, exist_ok=True)
    for target_name in targets:
        target = TARGETS[target_name]
        if target.name == "asyncmq":
            prepared.append({"target": target.name, "python": sys.executable, "packages": []})
            continue

        env_dir = root / target.name
        python = _venv_python(root, target.name)
        if not python.exists():
            venv.EnvBuilder(with_pip=True).create(env_dir)

        _run_subprocess([str(python), "-m", "pip", "install", "--upgrade", "pip"])
        _run_subprocess([str(python), "-m", "pip", "install", *target.packages])
        _run_subprocess([str(python), "-m", "pip", "check"])
        prepared.append({"target": target.name, "python": str(python), "packages": list(target.packages)})
    return prepared


def _resolve_target_python(target: str, root: Path, explicit: dict[str, str]) -> str:
    if target == "asyncmq":
        return sys.executable
    if target in explicit:
        return explicit[target]
    candidate = _venv_python(root, target)
    return str(candidate) if candidate.exists() else sys.executable


def _parse_target_python(values: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--target-python values must use TARGET=/path/to/python")
        target, python = value.split("=", 1)
        if target not in TARGETS:
            raise ValueError(f"unknown target for --target-python: {target}")
        result[target] = python
    return result


def _select_targets(raw_targets: str) -> list[str]:
    if raw_targets == "all":
        return list(TARGETS)
    selected = [item.strip() for item in raw_targets.split(",") if item.strip()]
    unknown = [target for target in selected if target not in TARGETS]
    if unknown:
        raise ValueError(f"unknown benchmark targets: {', '.join(unknown)}")
    return selected


def _workload_by_name(name: str):
    for workload in WORKLOADS:
        if workload.name == name:
            return workload
    raise ValueError(f"unknown benchmark workload: {name}")


@dataclass(frozen=True)
class RunDimensions:
    workload: str | None
    jobs: int
    workers: int
    concurrency: int
    payload_bytes: int
    warmup_jobs: int


def _run_dimensions(args: argparse.Namespace) -> RunDimensions:
    if args.workload is None:
        return RunDimensions(
            workload=None,
            jobs=args.jobs,
            workers=args.workers,
            concurrency=args.concurrency,
            payload_bytes=args.payload_bytes,
            warmup_jobs=args.warmup_jobs,
        )

    workload = _workload_by_name(args.workload)
    return RunDimensions(
        workload=workload.name,
        jobs=workload.jobs,
        workers=workload.workers,
        concurrency=workload.concurrency,
        payload_bytes=workload.payload_bytes,
        warmup_jobs=workload.warmup_jobs,
    )


async def _asyncmq_payload_task(payload: str, run_id: str) -> int:
    redis_url = os.environ.get("ASYNCMQ_BENCH_REDIS_URL", REDIS_URL)
    client = _asyncmq_counter_client(redis_url)
    pipe = client.pipeline()
    pipe.incr(_completion_key(run_id))
    pipe.incrby(_bytes_key(run_id), len(payload))
    await pipe.execute()
    return len(payload)


def _asyncmq_counter_client(redis_url: str) -> Any:
    client = _ASYNCMQ_COUNTERS.get(redis_url)
    if client is None:
        import redis.asyncio as redis

        client = redis.Redis(
            connection_pool=redis.BlockingConnectionPool.from_url(
                redis_url,
                max_connections=int(
                    os.environ.get("ASYNCMQ_BENCH_COUNTER_MAX_CONNECTIONS", ASYNCMQ_COUNTER_MAX_CONNECTIONS)
                ),
                timeout=float(os.environ.get("ASYNCMQ_BENCH_COUNTER_POOL_TIMEOUT", ASYNCMQ_COUNTER_POOL_TIMEOUT)),
            )
        )
        _ASYNCMQ_COUNTERS[redis_url] = client
    return client


async def _close_asyncmq_counter(redis_url: str) -> None:
    client = _ASYNCMQ_COUNTERS.pop(redis_url, None)
    if client is not None:
        await client.aclose(close_connection_pool=True)


async def _run_asyncmq_sample(
    *,
    jobs: int,
    workers: int,
    concurrency: int,
    payload_bytes: int,
    timeout: float,
    redis_url: str,
    queue: str,
    run_id: str,
) -> dict[str, int | float | bool | None]:
    from redis.asyncio import Redis

    payload = "x" * payload_bytes
    TASK_REGISTRY[ASYNCMQ_TASK_ID] = {"func": _asyncmq_payload_task}
    backend = RedisBackend(redis_url)
    counter = Redis.from_url(redis_url)
    await counter.flushdb()
    os.environ["ASYNCMQ_BENCH_REDIS_URL"] = redis_url

    try:
        cpu_user_start, cpu_system_start, _ = _resource_snapshot()
        async with anyio.create_task_group() as tg:
            worker_start = time.perf_counter_ns()
            for _ in range(workers):
                tg.start_soon(process_job, queue, anyio.CapacityLimiter(concurrency), None, backend)
            worker_startup_ns = time.perf_counter_ns() - worker_start

            enqueue_start = time.perf_counter_ns()
            total_start = enqueue_start
            for index in range(jobs):
                job = Job(task_id=ASYNCMQ_TASK_ID, args=[payload, run_id], kwargs={}, job_id=f"{run_id}-{index}")
                await backend.enqueue(queue, job.to_dict())
            enqueue_latency_ns = time.perf_counter_ns() - enqueue_start

            with anyio.move_on_after(timeout) as scope:
                while int(await counter.get(_completion_key(run_id)) or 0) < jobs:
                    await anyio.sleep(0.01)
            tg.cancel_scope.cancel()

        total_latency_ns = time.perf_counter_ns() - total_start
        completed = int(await counter.get(_completion_key(run_id)) or 0)
        throughput = completed / (total_latency_ns / 1_000_000_000) if total_latency_ns else 0.0
        cpu_user_end, cpu_system_end, max_rss_kb = _resource_snapshot()

        return {
            "enqueue_latency_ns": enqueue_latency_ns,
            "total_latency_ns": total_latency_ns,
            "throughput_jobs_per_second": throughput,
            "worker_startup_ns": worker_startup_ns,
            "cpu_user_seconds": (
                cpu_user_end - cpu_user_start if cpu_user_start is not None and cpu_user_end is not None else None
            ),
            "cpu_system_seconds": (
                cpu_system_end - cpu_system_start
                if cpu_system_start is not None and cpu_system_end is not None
                else None
            ),
            "max_rss_kb": max_rss_kb,
            "completed": completed,
            "failed": jobs - completed,
            "timed_out": scope.cancel_called,
        }
    finally:
        await backend.redis.aclose()
        await backend.job_store.redis.aclose()
        await counter.aclose()
        await _close_asyncmq_counter(redis_url)


def _worker_command(target: str, python: str, queue: str, concurrency: int, redis_url: str) -> list[str]:
    if target == "celery":
        return [
            python,
            "-m",
            "celery",
            "-A",
            "benchmarks.competitor_apps:celery_app",
            "worker",
            "--loglevel=WARNING",
            "--pool=threads",
            f"--concurrency={concurrency}",
            "-Q",
            queue,
        ]
    if target == "dramatiq":
        return [
            python,
            "-m",
            "dramatiq",
            "--processes",
            "1",
            "--threads",
            str(concurrency),
            "--queues",
            queue,
            "--path",
            os.getcwd(),
            "--skip-logging",
            "benchmarks.competitor_apps:dramatiq_broker",
            "benchmarks.competitor_apps",
        ]
    if target == "arq":
        return [python, "-m", "arq", "benchmarks.competitor_apps.ArqWorkerSettings"]
    if target == "rq":
        return [
            python,
            "-m",
            "rq.cli",
            "worker",
            "--url",
            redis_url,
            "--path",
            os.getcwd(),
            "--logging_level",
            "WARNING",
            queue,
        ]
    if target == "huey":
        return [
            python,
            "-m",
            "huey.bin.huey_consumer",
            "benchmarks.competitor_apps.huey",
            "--workers",
            str(concurrency),
            "--worker-type",
            "thread",
            "--delay",
            "0.01",
            "--max-delay",
            "0.1",
        ]
    raise ValueError(f"unsupported external target: {target}")


def _start_workers(
    *,
    target: str,
    python: str,
    workers: int,
    concurrency: int,
    redis_url: str,
    queue: str,
) -> list[subprocess.Popen[bytes]]:
    process_count = workers * concurrency if target == "rq" else workers
    command_concurrency = 1 if target == "rq" else concurrency
    env = _target_env(redis_url, queue, command_concurrency)
    return [
        subprocess.Popen(
            _worker_command(target, python, queue, command_concurrency, redis_url),
            cwd=os.getcwd(),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        for _ in range(process_count)
    ]


def _worker_exit_summary(processes: Sequence[subprocess.Popen[bytes]]) -> str:
    summaries: list[str] = []
    for index, process in enumerate(processes):
        if process.poll() is None:
            continue
        stderr = ""
        if process.stderr is not None:
            try:
                stderr = process.stderr.read().decode("utf-8", errors="replace").strip()
            except Exception:
                stderr = ""
        if len(stderr) > 2_000:
            stderr = stderr[-2_000:]
        summaries.append(f"worker={index} return_code={process.returncode} stderr={stderr!r}")
    return "; ".join(summaries)


def _stop_workers(processes: Sequence[subprocess.Popen[bytes]]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    deadline = time.monotonic() + 10
    for process in processes:
        while process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


async def _enqueue_external(
    target: str,
    *,
    target_python: str,
    jobs: int,
    payload_bytes: int,
    run_id: str,
    queue: str,
    redis_url: str,
    timeout: float,
) -> tuple[int, bool]:
    enqueue_start = time.perf_counter_ns()
    command = [
        target_python,
        "-c",
        "from benchmarks.competitor_apps import main; raise SystemExit(main())",
        "--target",
        target,
        "--jobs",
        str(jobs),
        "--payload-bytes",
        str(payload_bytes),
        "--run-id",
        run_id,
        "--queue",
        queue,
        "--redis-url",
        redis_url,
    ]
    env = _target_env(redis_url, queue, 1)
    try:
        await anyio.to_thread.run_sync(lambda: _run_subprocess(command, env=env, timeout=timeout))
    except subprocess.TimeoutExpired:
        return time.perf_counter_ns() - enqueue_start, True
    return time.perf_counter_ns() - enqueue_start, False


async def _wait_for_completion_counter(
    counter: Any,
    key: str,
    *,
    jobs: int,
    timeout: float,
    no_progress_timeout: float,
) -> bool:
    deadline = time.monotonic() + timeout
    no_progress_deadline = time.monotonic() + no_progress_timeout if no_progress_timeout > 0 else None
    completed = int(counter.get(key) or 0)

    while completed < jobs:
        now = time.monotonic()
        if now >= deadline:
            return True
        if no_progress_deadline is not None and now >= no_progress_deadline:
            return True

        await anyio.sleep(0.01)
        next_completed = int(counter.get(key) or 0)
        if next_completed > completed:
            completed = next_completed
            if no_progress_timeout > 0:
                no_progress_deadline = time.monotonic() + no_progress_timeout

    return False


async def _run_external_sample(
    *,
    target: str,
    target_python: str,
    jobs: int,
    workers: int,
    concurrency: int,
    payload_bytes: int,
    timeout: float,
    redis_url: str,
    queue: str,
    run_id: str,
    worker_startup_delay: float,
    no_progress_timeout: float,
) -> dict[str, int | float | bool | None]:
    from redis import Redis

    counter = Redis.from_url(redis_url)
    counter.flushdb()
    cpu_user_start, cpu_system_start, _ = _resource_snapshot()

    worker_start = time.perf_counter_ns()
    processes = _start_workers(
        target=target,
        python=target_python,
        workers=workers,
        concurrency=concurrency,
        redis_url=redis_url,
        queue=queue,
    )
    try:
        await anyio.sleep(worker_startup_delay)
        exited = [process.returncode for process in processes if process.poll() is not None]
        if exited:
            raise RuntimeError(
                f"{target} worker exited before jobs were enqueued: "
                f"return_codes={exited}; {_worker_exit_summary(processes)}"
            )
        worker_startup_ns = time.perf_counter_ns() - worker_start
        total_start = time.perf_counter_ns()
        enqueue_latency_ns, enqueue_timed_out = await _enqueue_external(
            target,
            target_python=target_python,
            jobs=jobs,
            payload_bytes=payload_bytes,
            run_id=run_id,
            queue=queue,
            redis_url=redis_url,
            timeout=timeout,
        )

        timed_out = enqueue_timed_out or await _wait_for_completion_counter(
            counter,
            _completion_key(run_id),
            jobs=jobs,
            timeout=timeout,
            no_progress_timeout=no_progress_timeout,
        )
        total_latency_ns = time.perf_counter_ns() - total_start
    finally:
        _stop_workers(processes)

    completed = int(counter.get(_completion_key(run_id)) or 0)
    throughput = completed / (total_latency_ns / 1_000_000_000) if total_latency_ns else 0.0
    cpu_user_end, cpu_system_end, max_rss_kb = _resource_snapshot()
    counter.close()
    return {
        "enqueue_latency_ns": enqueue_latency_ns,
        "total_latency_ns": total_latency_ns,
        "throughput_jobs_per_second": throughput,
        "worker_startup_ns": worker_startup_ns,
        "cpu_user_seconds": (
            cpu_user_end - cpu_user_start if cpu_user_start is not None and cpu_user_end is not None else None
        ),
        "cpu_system_seconds": (
            cpu_system_end - cpu_system_start if cpu_system_start is not None and cpu_system_end is not None else None
        ),
        "max_rss_kb": max_rss_kb,
        "completed": completed,
        "failed": jobs - completed,
        "timed_out": timed_out,
    }


async def run_target(
    *,
    target: str,
    target_python: str,
    workload: str | None,
    jobs: int,
    workers: int,
    concurrency: int,
    payload_bytes: int,
    timeout: float,
    warmup_jobs: int,
    repetitions: int,
    redis_url: str,
    worker_startup_delay: float,
    no_progress_timeout: float = 30.0,
) -> CompetitiveResult:
    if target not in TARGETS:
        raise ValueError(f"unknown target: {target}")
    if jobs <= 0 or workers <= 0 or concurrency <= 0 or repetitions <= 0:
        raise ValueError("jobs, workers, concurrency, and repetitions must be greater than zero")
    if payload_bytes < 0 or warmup_jobs < 0:
        raise ValueError("payload_bytes and warmup_jobs must be greater than or equal to zero")

    queue = f"asyncmq-benchmark-{target}-{uuid4().hex}"
    if warmup_jobs:
        run_id = f"warmup-{uuid4().hex}"
        if target == "asyncmq":
            await _run_asyncmq_sample(
                jobs=warmup_jobs,
                workers=workers,
                concurrency=concurrency,
                payload_bytes=payload_bytes,
                timeout=timeout,
                redis_url=redis_url,
                queue=queue,
                run_id=run_id,
            )
        else:
            await _run_external_sample(
                target=target,
                target_python=target_python,
                jobs=warmup_jobs,
                workers=workers,
                concurrency=concurrency,
                payload_bytes=payload_bytes,
                timeout=timeout,
                redis_url=redis_url,
                queue=queue,
                run_id=run_id,
                worker_startup_delay=worker_startup_delay,
                no_progress_timeout=no_progress_timeout,
            )

    samples: list[dict[str, int | float | bool | None]] = []
    for _ in range(repetitions):
        run_id = f"sample-{uuid4().hex}"
        if target == "asyncmq":
            sample = await _run_asyncmq_sample(
                jobs=jobs,
                workers=workers,
                concurrency=concurrency,
                payload_bytes=payload_bytes,
                timeout=timeout,
                redis_url=redis_url,
                queue=queue,
                run_id=run_id,
            )
        else:
            sample = await _run_external_sample(
                target=target,
                target_python=target_python,
                jobs=jobs,
                workers=workers,
                concurrency=concurrency,
                payload_bytes=payload_bytes,
                timeout=timeout,
                redis_url=redis_url,
                queue=queue,
                run_id=run_id,
                worker_startup_delay=worker_startup_delay,
                no_progress_timeout=no_progress_timeout,
            )
        samples.append(sample)

    stats = _metric_statistics(samples)
    return CompetitiveResult(
        target=target,
        workload=workload,
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
        worker_startup_ns=int(stats["worker_startup_ns"]["median"]),
        cpu_user_seconds=(float(stats["cpu_user_seconds"]["median"]) if "cpu_user_seconds" in stats else None),
        cpu_system_seconds=(float(stats["cpu_system_seconds"]["median"]) if "cpu_system_seconds" in stats else None),
        max_rss_kb=int(stats["max_rss_kb"]["max"]) if "max_rss_kb" in stats else None,
        completed=int(min(sample["completed"] or 0 for sample in samples)),
        failed=int(max(sample["failed"] or 0 for sample in samples)),
        timed_out=any(bool(sample.get("timed_out")) for sample in samples),
        samples=samples,
        statistics=stats,
    )


def benchmark_inventory() -> dict[str, Any]:
    return {
        "already_exists": [
            "benchmarks.plan canonical workloads and competitor list",
            "CodSpeed microbenchmarks for job and InMemoryBackend operations",
            "benchmarks.load_asyncmq JSON load runner with warmup/repetitions/statistics",
        ],
        "reused": [
            "workload dimensions",
            "time.perf_counter_ns timing policy",
            "median/p95/p99/min/max statistics",
            "CPU and max RSS sampling",
            "Hatch benchmark command style",
        ],
        "added": [
            "Redis-backed competitive runner",
            "shared completion signal across target queues",
            "isolated per-target virtualenv preparation",
            "target Python selection for dependency-conflicting competitors",
        ],
        "why_new_work_was_unavoidable": [
            "existing code only benchmarked AsyncMQ internals and an AsyncMQ in-memory load path",
            "competitors were listed for availability but had no executable workload adapter",
            "Arq's redis<6 dependency conflicts with AsyncMQ's redis>=7 dependency in one interpreter",
        ],
    }


async def run_competitive(args: argparse.Namespace) -> list[CompetitiveResult]:
    selected = _select_targets(args.targets)
    target_pythons = _parse_target_python(args.target_python)
    dimensions = _run_dimensions(args)
    return [
        await run_target(
            target=target,
            target_python=_resolve_target_python(target, args.venv_root, target_pythons),
            workload=dimensions.workload,
            jobs=dimensions.jobs,
            workers=dimensions.workers,
            concurrency=dimensions.concurrency,
            payload_bytes=dimensions.payload_bytes,
            timeout=args.timeout,
            warmup_jobs=dimensions.warmup_jobs,
            repetitions=args.repetitions,
            redis_url=args.redis_url,
            worker_startup_delay=args.worker_startup_delay,
            no_progress_timeout=args.no_progress_timeout,
        )
        for target in selected
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run comparable Redis-backed queue benchmarks.")
    parser.add_argument("--targets", default="asyncmq", help="Comma-separated targets or 'all'.")
    parser.add_argument(
        "--workload",
        choices=[workload.name for workload in WORKLOADS],
        help="Use a named workload from benchmarks.plan.",
    )
    parser.add_argument("--jobs", type=int, default=1_000)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--warmup-jobs", type=int, default=0)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--redis-url", default=REDIS_URL)
    parser.add_argument("--worker-startup-delay", type=float, default=4.0)
    parser.add_argument("--no-progress-timeout", type=float, default=30.0)
    parser.add_argument("--venv-root", type=Path, default=Path(".benchmarks/envs"))
    parser.add_argument("--target-python", action="append", default=[], help="TARGET=/path/to/python override.")
    parser.add_argument(
        "--prepare-envs", action="store_true", help="Create isolated competitor virtualenvs and install packages."
    )
    parser.add_argument("--inventory", action="store_true", help="Report benchmark infrastructure audit data.")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print selected targets and environment paths without running jobs."
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args(argv)

    selected = _select_targets(args.targets)
    payload: dict[str, Any] = {"inventory": benchmark_inventory() if args.inventory else None}
    if args.prepare_envs:
        payload["prepared"] = prepare_envs(selected, args.venv_root)
    if args.dry_run:
        explicit = _parse_target_python(args.target_python)
        dimensions = _run_dimensions(args)
        payload["dry_run"] = [
            {
                "concurrency": dimensions.concurrency,
                "jobs": dimensions.jobs,
                "packages": list(TARGETS[target].packages),
                "payload_bytes": dimensions.payload_bytes,
                "python": _resolve_target_python(target, args.venv_root, explicit),
                "target": target,
                "warmup_jobs": dimensions.warmup_jobs,
                "workers": dimensions.workers,
                "workload": dimensions.workload,
            }
            for target in selected
        ]
    if not args.prepare_envs and not args.dry_run:
        payload["results"] = [asdict(result) for result in asyncio.run(run_competitive(args))]

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        if payload.get("inventory"):
            print("Benchmark inventory")
            for key, values in payload["inventory"].items():
                print(f"{key}:")
                for value in values:
                    print(f"  - {value}")
        for item in payload.get("prepared", []):
            print(f"prepared {item['target']}: {item['python']}")
        for item in payload.get("dry_run", []):
            print(f"{item['target']}: python={item['python']} packages={item['packages']}")
        for item in payload.get("results", []):
            print(
                "{target}: completed={completed} throughput={throughput_jobs_per_second:.2f}/s "
                "enqueue_ns={enqueue_latency_ns}".format(**item)
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
