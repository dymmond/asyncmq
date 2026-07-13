import json
import sys

import pytest

from benchmarks.competitive import (
    TARGETS,
    _parse_target_python,
    _select_targets,
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
    exit_code = main(["--targets", "asyncmq,rq", "--dry-run", "--inventory", "--json"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"][0]["target"] == "asyncmq"
    assert payload["dry_run"][0]["python"] == sys.executable
    assert payload["dry_run"][1]["target"] == "rq"
    assert payload["inventory"]["added"]


async def test_competitive_asyncmq_runner_processes_all_jobs():
    result = await run_target(
        target="asyncmq",
        target_python=sys.executable,
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
    assert result.failed == 0
    assert result.total_concurrency == 2
    assert len(result.samples) == 2
    assert result.throughput_jobs_per_second > 0
