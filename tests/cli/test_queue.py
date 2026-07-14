import pytest
from click.testing import CliRunner

from asyncmq.cli.__main__ import app

runner = CliRunner()


class FakeBackend:
    def __init__(self):
        self.queues = {"queue1": [{}], "queue2": [{}]}
        self.delayed = {"queue1": [], "queue2": []}
        self.dlqs = {"queue1": [], "queue2": []}
        self.calls = []

    async def is_queue_paused(self, queue):
        return False

    async def drain_queue(self, queue, *, include_delayed=False):
        self.calls.append(("drain", queue, include_delayed))
        return ["waiting-1", "delayed-1"] if include_delayed else ["waiting-1"]

    async def clean_jobs(self, queue, *, state, grace, limit):
        self.calls.append(("clean", queue, state, grace, limit))
        return ["completed-1"]

    async def obliterate_queue(self, queue, *, force=False):
        self.calls.append(("obliterate", queue, force))
        if not force:
            raise RuntimeError("force required")
        return ["job-1", "job-2"]


class MarkupQueueBackend(FakeBackend):
    async def list_queues(self):
        return ["[bold red]prod[/]"]


@pytest.fixture
def fake_backend():
    from asyncmq.conf import settings

    original_backend = settings.backend
    backend = FakeBackend()
    settings.backend = backend
    yield backend
    settings.backend = original_backend


def test_queue_help():
    result = runner.invoke(app, ["queue", "--help"])
    assert result.exit_code == 0
    assert "Manages AsyncMQ queues." in result.output


def test_queue_list(monkeypatch, fake_backend):
    result = runner.invoke(app, ["queue", "list"])
    assert result.exit_code == 0
    assert "Queue listing not supported for this backend" in result.output


def test_queue_list_escapes_rich_markup_identifiers():
    from asyncmq.conf import settings

    original_backend = settings.backend
    settings.backend = MarkupQueueBackend()
    try:
        result = runner.invoke(app, ["queue", "list"])
    finally:
        settings.backend = original_backend

    assert result.exit_code == 0
    assert "[bold red]prod[/]" in result.output


def test_queue_info(monkeypatch, fake_backend):
    result = runner.invoke(app, ["queue", "info", "queue1"])
    assert result.exit_code == 0
    assert "Paused" in result.output
    assert "Waiting Jobs" in result.output


def test_queue_drain(monkeypatch, fake_backend):
    result = runner.invoke(app, ["queue", "drain", "queue1", "--include-delayed"])

    assert result.exit_code == 0
    assert "Drain removed 2 job(s)" in result.output
    assert ("drain", "queue1", True) in fake_backend.calls


def test_queue_clean(monkeypatch, fake_backend):
    result = runner.invoke(
        app,
        ["queue", "clean", "queue1", "--state", "completed", "--grace", "60", "--limit", "10"],
    )

    assert result.exit_code == 0
    assert "Clean removed 1 job(s)" in result.output
    assert ("clean", "queue1", "completed", 60.0, 10) in fake_backend.calls


def test_queue_obliterate_requires_force(monkeypatch, fake_backend):
    result = runner.invoke(app, ["queue", "obliterate", "queue1"])

    assert result.exit_code != 0
    assert "force required" in str(result.exception)


def test_queue_obliterate_with_force(monkeypatch, fake_backend):
    result = runner.invoke(app, ["queue", "obliterate", "queue1", "--force"])

    assert result.exit_code == 0
    assert "Obliterate removed 2 job(s)" in result.output
    assert ("obliterate", "queue1", True) in fake_backend.calls
