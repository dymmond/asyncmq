from click.testing import CliRunner

from asyncmq.cli.__main__ import app

runner = CliRunner()

class FakeBackend:
    queues = {"queue1": [{}], "queue2": [{}]}
    delayed = {"queue1": [], "queue2": []}
    dlqs = {"queue1": [], "queue2": []}

    async def is_queue_paused(self, queue):
        return False

def test_queue_help():
    result = runner.invoke(app, ["queue", "--help"])
    assert result.exit_code == 0
    assert "Manages AsyncMQ queues." in result.output

def test_queue_list(monkeypatch):
    from asyncmq.conf import settings
    settings.backend = FakeBackend()

    result = runner.invoke(app, ["queue", "list"])
    assert result.exit_code == 0
    assert "queue1" in result.output
    assert "queue2" in result.output

def test_queue_info(monkeypatch):
    from asyncmq.conf import settings
    settings.backend = FakeBackend()

    result = runner.invoke(app, ["queue", "info", "queue1"])
    assert result.exit_code == 0
    assert "Paused" in result.output
    assert "Waiting Jobs" in result.output
