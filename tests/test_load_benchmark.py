import pytest

from benchmarks.load_asyncmq import main, run_load

pytestmark = pytest.mark.anyio


async def test_load_runner_processes_all_jobs():
    result = await run_load(jobs=25, workers=5, concurrency=1, payload_bytes=32, timeout=5.0)

    assert result.completed == 25
    assert result.failed == 0
    assert result.repetitions == 1
    assert result.samples[0]["completed"] == 25
    assert "total_latency_ns" in result.statistics
    assert result.throughput_jobs_per_second > 0
    assert result.cpu_user_seconds is None or result.cpu_user_seconds >= 0
    assert result.cpu_system_seconds is None or result.cpu_system_seconds >= 0
    assert result.max_rss_kb is None or result.max_rss_kb > 0


async def test_load_runner_records_repeated_samples_after_warmup():
    result = await run_load(
        jobs=10,
        workers=2,
        concurrency=1,
        payload_bytes=16,
        timeout=5.0,
        warmup_jobs=3,
        repetitions=3,
    )

    assert result.warmup_jobs == 3
    assert result.repetitions == 3
    assert len(result.samples) == 3
    assert all(sample["completed"] == 10 for sample in result.samples)
    assert result.statistics["throughput_jobs_per_second"]["median"] > 0
    assert set(result.statistics["total_latency_ns"]) == {"median", "p95", "p99", "min", "max"}


def test_load_runner_cli_outputs_json(capsys):
    exit_code = main(
        [
            "--jobs",
            "5",
            "--workers",
            "2",
            "--payload-bytes",
            "16",
            "--warmup-jobs",
            "1",
            "--repetitions",
            "2",
            "--json",
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert '"completed": 5' in output
    assert '"repetitions": 2' in output
    assert '"statistics":' in output
    assert '"max_rss_kb":' in output
