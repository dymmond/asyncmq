import anyio
from click.testing import CliRunner

from asyncmq import InMemoryBackend
from asyncmq.cli.__main__ import app

runner = CliRunner()


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


def test_job_help():
    result = runner.invoke(app, ["job", "--help"])
    assert result.exit_code == 0
    assert "Manages AsyncMQ jobs within queue" in result.output


def test_job_inspect(monkeypatch):
    from asyncmq.conf import monkay

    monkay.settings.backend = FakeBackend()

    result = runner.invoke(app, ["job", "inspect", "job1", "--queue", "queue1"])
    assert result.exit_code == 0
    assert "job1" in result.output


def test_job_retry(monkeypatch):
    from asyncmq.conf import monkay

    monkay.settings.backend = FakeBackend()

    result = runner.invoke(app, ["job", "retry", "job1", "--queue", "queue1"])
    assert result.exit_code == 0
    assert "Retrying job" in result.output


def test_job_remove(monkeypatch):
    from asyncmq.conf import monkay

    monkay.settings.backend = FakeBackend()

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

    from asyncmq.conf import monkay

    monkay.settings.backend = anyio.run(setup_test_backend)

    result = runner.invoke(app, ["job", "list", "--queue", "queue1", "--state", "waiting"])

    assert result.exit_code == 0
    assert "ID: job1  State: waiting  Task: task1" in result.output
