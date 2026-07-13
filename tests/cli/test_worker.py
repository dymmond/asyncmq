from click.testing import CliRunner

from asyncmq.backends.base import WorkerInfo
from asyncmq.cli.__main__ import app

runner = CliRunner()


class FakeWorkerBackend:
    async def list_workers(self):
        return [
            WorkerInfo(id="worker-1", queue="emails", concurrency=3, heartbeat=1_700_000_000.0),
        ]


def test_worker_inspect():
    from asyncmq.conf import settings

    original_backend = settings.backend
    settings.backend = FakeWorkerBackend()
    try:
        result = runner.invoke(app, ["worker", "inspect", "worker-1"])
    finally:
        settings.backend = original_backend

    assert result.exit_code == 0
    assert "worker-1" in result.output
    assert "emails" in result.output
    assert "3" in result.output


def test_worker_inspect_missing():
    from asyncmq.conf import settings

    original_backend = settings.backend
    settings.backend = FakeWorkerBackend()
    try:
        result = runner.invoke(app, ["worker", "inspect", "missing"])
    finally:
        settings.backend = original_backend

    assert result.exit_code != 0
    assert "Worker 'missing' was not found" in result.output


def test_worker_help():
    result = runner.invoke(app, ["worker", "--help"])
    assert result.exit_code == 0
    assert "Manages AsyncMQ worker processes" in result.output


def xtest_worker_start_logo(monkeypatch):
    def fake_start_worker(*args, **kwargs):
        return 0

    monkeypatch.setattr("asyncmq.runners.start_worker", fake_start_worker)

    result = runner.invoke(app, ["worker", "start", "myqueue", "--concurrency", "2"])

    assert result.exit_code == 0
    assert "AsyncMQ Worker" in result.output
    assert "Queue: 'myqueue'" in result.output
