import signal

import anyio
import pytest
from click.testing import CliRunner

from asyncmq.backends.base import WorkerInfo
from asyncmq.cli.__main__ import app
from asyncmq.cli.worker import _signal_drain_loop

runner = CliRunner()


class FakeWorkerBackend:
    async def list_workers(self):
        return [
            WorkerInfo(id="worker-1", queue="emails", concurrency=3, heartbeat=1_700_000_000.0),
        ]


class MarkupWorkerBackend:
    async def list_workers(self):
        return [
            WorkerInfo(id="[bold]worker[/]", queue="[red]emails[/]", concurrency=3, heartbeat=1_700_000_000.0),
        ]


class FakeSignalStream:
    def __init__(self, signum: signal.Signals) -> None:
        self.signum = signum
        self.sent = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.sent:
            raise StopAsyncIteration
        self.sent = True
        return self.signum


@pytest.mark.anyio
async def test_worker_signal_loop_requests_drain_on_sigterm():
    drain_event = anyio.Event()

    await _signal_drain_loop(drain_event, FakeSignalStream(signal.SIGTERM))

    assert drain_event.is_set()


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


def test_worker_inspect_escapes_rich_markup_identifiers():
    from asyncmq.conf import settings

    original_backend = settings.backend
    settings.backend = MarkupWorkerBackend()
    try:
        result = runner.invoke(app, ["worker", "inspect", "[bold]worker[/]"])
    finally:
        settings.backend = original_backend

    assert result.exit_code == 0
    assert "[bold]worker[/]" in result.output
    assert "[red]emails[/]" in result.output


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
