from click.testing import CliRunner

from asyncmq.cli.__main__ import app

runner = CliRunner()


def test_info_version():
    result = runner.invoke(app, ["info", "version"])
    assert result.exit_code == 0
    assert "AsyncMQ version" in result.output
