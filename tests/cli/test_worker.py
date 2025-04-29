from click.testing import CliRunner

from asyncmq.cli.__main__ import app

runner = CliRunner()


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
