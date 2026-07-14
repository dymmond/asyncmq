from __future__ import annotations

import json
import logging

from asyncmq.conf.global_settings import Settings
from asyncmq.core.utils.logging import JSONLogFormatter, StandardLoggingConfig


def test_json_log_formatter_emits_machine_readable_payload() -> None:
    formatter = JSONLogFormatter()
    record = logging.LogRecord(
        name="asyncmq.worker",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="processed %s",
        args=("job",),
        exc_info=None,
        func=None,
    )
    record.queue = "emails"
    record.job_id = "job-1"

    payload = json.loads(formatter.format(record))

    assert payload["level"] == "INFO"
    assert payload["logger"] == "asyncmq.worker"
    assert payload["message"] == "processed job"
    assert payload["queue"] == "emails"
    assert payload["job_id"] == "job-1"
    assert "timestamp" in payload


def test_json_log_formatter_preserves_stack_info() -> None:
    formatter = JSONLogFormatter()
    record = logging.LogRecord(
        name="asyncmq.worker",
        level=logging.ERROR,
        pathname=__file__,
        lineno=10,
        msg="worker stalled",
        args=(),
        exc_info=None,
        func=None,
        sinfo="Stack (most recent call last):\n  asyncmq frame",
    )

    payload = json.loads(formatter.format(record))

    assert payload["stack_info"] == "Stack (most recent call last):\n  asyncmq frame"


def test_standard_logging_config_can_emit_json_formatter() -> None:
    config = StandardLoggingConfig(level="INFO", structured=True)

    assert config.config["formatters"]["default"] == {
        "()": "asyncmq.core.utils.logging.JSONLogFormatter",
    }


def test_settings_select_structured_logging_config() -> None:
    settings = Settings(structured_logging=True)

    assert isinstance(settings.logging_config, StandardLoggingConfig)
    assert settings.logging_config.structured is True
