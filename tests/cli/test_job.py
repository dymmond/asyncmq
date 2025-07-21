import anyio
import pytest
from click.testing import CliRunner

from asyncmq import InMemoryBackend
from asyncmq.cli.__main__ import app
from asyncmq.core.dependencies import get_backend, get_settings

runner = CliRunner()


@pytest.fixture(autouse=True)
def reset_cache():
    get_backend.cache_clear()
    get_settings.cache_clear()


class FakeBackend:
    queues = {}
    job_store = {}

    def __init__(self):
        self.job_store = self

    async def load(self, queue, job_id):
        return {"id": job_id, "queue": queue, "status": "waiting"}

    async def delete(self, queue, job_id):
        return

    async def enqueue(self, queue, job):
        return


@pytest.fixture
def fake_backend():
    from asyncmq.conf import settings

    original_backend = settings.backend
    settings.backend = FakeBackend()
    yield
    settings.backend = original_backend


def test_job_help():
    result = runner.invoke(app, ["job", "--help"])
    assert result.exit_code == 0
    assert "Manages AsyncMQ jobs within queue" in result.output


def test_job_inspect(monkeypatch, fake_backend):
    result = runner.invoke(app, ["job", "inspect", "job1", "--queue", "queue1"])
    assert result.exit_code == 0
    assert "job1" in result.output


def test_job_retry(monkeypatch, fake_backend):
    result = runner.invoke(app, ["job", "retry", "job1", "--queue", "queue1"])
    assert result.exit_code == 0
    assert "Retrying job" in result.output


def test_job_remove(monkeypatch, fake_backend):
    result = runner.invoke(app, ["job", "remove", "job1", "--queue", "queue1"])
    assert result.exit_code == 0
    assert "Deleted job" in result.output


def test_job_list(monkeypatch):
    async def setup_test_backend():
        backend = InMemoryBackend()

        test_jobs = [
            {"id": "job1", "task": "task1", "status": "waiting", "priority": 5},
            {"id": "job2", "task": "task2", "status": "waiting", "priority": 5},
        ]

        for job in test_jobs:
            await backend.enqueue("queue1", job)

        return backend

    from asyncmq.conf import settings

    settings.backend = anyio.run(setup_test_backend)

    result = runner.invoke(app, ["job", "list", "--queue", "queue1", "--state", "waiting"])

    assert result.exit_code == 0
