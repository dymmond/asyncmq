import pytest
from click.testing import CliRunner

from asyncmq.cli.__main__ import app
from asyncmq.core.dependencies import get_backend, get_settings

runner = CliRunner()


@pytest.fixture(autouse=True)
def reset_cache():
    get_backend.cache_clear()
    get_settings.cache_clear()


class FakeBackend:
    queues = {"queue1": [{}], "queue2": [{}]}
    delayed = {"queue1": [], "queue2": []}
    dlqs = {"queue1": [], "queue2": []}

    async def is_queue_paused(self, queue):
        return False


@pytest.fixture
def fake_backend():
    from asyncmq.conf import settings

    original_backend = settings.backend
    settings.backend = FakeBackend()
    yield
    settings.backend = original_backend


def test_queue_help():
    result = runner.invoke(app, ["queue", "--help"])
    assert result.exit_code == 0
    assert "Manages AsyncMQ queues." in result.output


def test_queue_list(monkeypatch, fake_backend):
    result = runner.invoke(app, ["queue", "list"])
    assert result.exit_code == 0
    assert "Queue listing not supported for this backend" in result.output


def test_queue_info(monkeypatch, fake_backend):
    result = runner.invoke(app, ["queue", "info", "queue1"])
    assert result.exit_code == 0
    assert "Paused" in result.output
    assert "Waiting Jobs" in result.output
