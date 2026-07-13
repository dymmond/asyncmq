import json
import subprocess
import sys

import pytest
import redis.asyncio as async_redis

import benchmarks.competitive as competitive
from benchmarks.competitive import (
    TARGETS,
    _asyncmq_counter_client,
    _close_asyncmq_counter,
    _parse_target_python,
    _select_targets,
    _wait_for_completion_counter,
    _worker_exit_summary,
    benchmark_inventory,
    main,
    run_target,
)

pytestmark = pytest.mark.anyio


def test_competitive_targets_cover_required_systems():
    assert set(TARGETS) == {"asyncmq", "celery", "dramatiq", "arq", "rq", "huey"}
    assert TARGETS["arq"].packages == ("arq>=0.28,<0.29",)
    assert TARGETS["huey"].packages == ("huey>=3.2,<4", "redis>=5,<6")


def test_competitive_target_selection_and_python_overrides():
    assert _select_targets("all") == ["asyncmq", "celery", "dramatiq", "arq", "rq", "huey"]
    assert _select_targets("asyncmq,rq") == ["asyncmq", "rq"]
    assert _parse_target_python(["rq=/tmp/rq-python"]) == {"rq": "/tmp/rq-python"}

    with pytest.raises(ValueError, match="unknown benchmark targets"):
        _select_targets("missing")
    with pytest.raises(ValueError, match="TARGET=/path/to/python"):
        _parse_target_python(["rq"])


def test_competitive_inventory_documents_reuse_and_gaps():
    inventory = benchmark_inventory()

    assert "benchmarks.plan canonical workloads and competitor list" in inventory["already_exists"]
    assert "workload dimensions" in inventory["reused"]
    assert "isolated per-target virtualenv preparation" in inventory["added"]
    assert any("Arq" in reason for reason in inventory["why_new_work_was_unavoidable"])


def test_competitive_cli_dry_run_outputs_json(capsys):
    exit_code = main(["--targets", "asyncmq,rq", "--workload", "small-payload", "--dry-run", "--inventory", "--json"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"][0]["target"] == "asyncmq"
    assert payload["dry_run"][0]["python"] == sys.executable
    assert payload["dry_run"][0]["workload"] == "small-payload"
    assert payload["dry_run"][0]["jobs"] == 10_000
    assert payload["dry_run"][0]["warmup_jobs"] == 1_000
    assert payload["dry_run"][0]["concurrency"] == 10
    assert payload["dry_run"][1]["target"] == "rq"
    assert payload["inventory"]["added"]


def test_worker_exit_summary_includes_stderr():
    process = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stderr.write('worker boom'); raise SystemExit(7)"],
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
    )
    process.wait(timeout=5)

    summary = _worker_exit_summary([process])

    assert "return_code=7" in summary
    assert "worker boom" in summary


async def test_completion_counter_no_progress_timeout_bounds_stalled_samples():
    class FrozenCounter:
        def get(self, key):
            return 1

    timed_out = await _wait_for_completion_counter(
        FrozenCounter(),
        "completion-key",
        jobs=5,
        timeout=60.0,
        no_progress_timeout=0.01,
    )

    assert timed_out is True


async def test_external_enqueue_timeout_bounds_blocked_producers(monkeypatch):
    def fake_run_subprocess(command, *, env=None, timeout=None):
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(competitive, "_run_subprocess", fake_run_subprocess)

    enqueue_latency_ns, timed_out = await competitive._enqueue_external(
        "rq",
        target_python=sys.executable,
        jobs=5,
        payload_bytes=16,
        run_id="blocked-producer",
        queue="blocked-producer-queue",
        redis_url="redis://localhost:6379/15",
        timeout=0.01,
    )

    assert enqueue_latency_ns > 0
    assert timed_out is True


async def test_competitive_asyncmq_runner_processes_all_jobs():
    result = await run_target(
        target="asyncmq",
        target_python=sys.executable,
        workload="unit-smoke",
        jobs=5,
        workers=2,
        concurrency=1,
        payload_bytes=16,
        timeout=5.0,
        warmup_jobs=1,
        repetitions=2,
        redis_url="redis://localhost:6379/15",
        worker_startup_delay=0.0,
    )

    assert result.completed == 5
    assert result.workload == "unit-smoke"
    assert result.failed == 0
    assert result.total_concurrency == 2
    assert len(result.samples) == 2
    assert result.throughput_jobs_per_second > 0


async def test_asyncmq_benchmark_counter_uses_blocking_pool():
    redis_url = "redis://localhost:6379/15"
    client = _asyncmq_counter_client(redis_url)
    try:
        assert isinstance(client.connection_pool, async_redis.BlockingConnectionPool)
    finally:
        await _close_asyncmq_counter(redis_url)


async def test_competitive_runner_records_timed_out_samples(monkeypatch):
    async def fake_sample(**kwargs):
        return {
            "enqueue_latency_ns": 1,
            "total_latency_ns": 10,
            "throughput_jobs_per_second": 0.1,
            "worker_startup_ns": 1,
            "cpu_user_seconds": 0.0,
            "cpu_system_seconds": 0.0,
            "max_rss_kb": 1,
            "completed": 1,
            "failed": 4,
            "timed_out": True,
        }

    monkeypatch.setattr(competitive, "_run_asyncmq_sample", fake_sample)

    result = await run_target(
        target="asyncmq",
        target_python=sys.executable,
        workload="unit-timeout",
        jobs=5,
        workers=1,
        concurrency=1,
        payload_bytes=16,
        timeout=0.01,
        warmup_jobs=0,
        repetitions=1,
        redis_url="redis://localhost:6379/15",
        worker_startup_delay=0.0,
    )

    assert result.timed_out is True
    assert result.completed == 1
    assert result.failed == 4
    assert result.samples[0]["timed_out"] is True
