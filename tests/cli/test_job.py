from click.testing import CliRunner

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
    assert "Job management commands" in result.output

def test_job_inspect(monkeypatch):
    from asyncmq.conf import settings
    settings.backend = FakeBackend()

    result = runner.invoke(app, ["job", "inspect", "job1", "--queue", "queue1"])
    assert result.exit_code == 0
    assert "job1" in result.output

def test_job_retry(monkeypatch):
    from asyncmq.conf import settings
    settings.backend = FakeBackend()

    result = runner.invoke(app, ["job", "retry", "job1", "--queue", "queue1"])
    assert result.exit_code == 0
    assert "Retrying job" in result.output

def test_job_remove(monkeypatch):
    from asyncmq.conf import settings
    settings.backend = FakeBackend()

    result = runner.invoke(app, ["job", "remove", "job1", "--queue", "queue1"])
    assert result.exit_code == 0
    assert "Deleted job" in result.output
