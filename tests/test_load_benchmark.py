import pytest

from benchmarks.load_asyncmq import main, run_load

pytestmark = pytest.mark.anyio


async def test_load_runner_processes_all_jobs():
    result = await run_load(jobs=25, workers=5, concurrency=1, payload_bytes=32, timeout=5.0)

    assert result.completed == 25
    assert result.failed == 0
    assert result.throughput_jobs_per_second > 0
    assert result.cpu_user_seconds is None or result.cpu_user_seconds >= 0
    assert result.cpu_system_seconds is None or result.cpu_system_seconds >= 0
    assert result.max_rss_kb is None or result.max_rss_kb > 0


def test_load_runner_cli_outputs_json(capsys):
    exit_code = main(["--jobs", "5", "--workers", "2", "--payload-bytes", "16", "--json"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert '"completed": 5' in output
    assert '"max_rss_kb":' in output
