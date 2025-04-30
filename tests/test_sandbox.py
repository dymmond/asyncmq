import time

import pytest

from asyncmq import sandbox
from asyncmq.tasks import TASK_REGISTRY, task


@task(queue="test")
def add():
    return 42


@task(queue="test")
async def say_hi():
    return "hi"


@task(queue="test")
def boom():
    raise ValueError("explode")


def test_success_handler_returns_value():
    task_id = [k for k, v in TASK_REGISTRY.items() if v["func"] == add][0]
    result = sandbox.run_handler(task_id, (), {}, timeout=1)
    assert result == 42


def test_async_handler_returns_value():
    task_id = [k for k, v in TASK_REGISTRY.items() if v["func"] == say_hi][0]
    result = sandbox.run_handler(task_id, (), {}, timeout=1)
    assert result == "hi"


def test_handler_exception_propagated():
    task_id = [k for k, v in TASK_REGISTRY.items() if v["func"] == boom][0]
    with pytest.raises(RuntimeError) as exc:
        sandbox.run_handler(task_id, (), {}, timeout=1)
    assert "explode" in str(exc.value)


def test_missing_task_raises_key_error():
    TASK_REGISTRY.pop("no_task", None)
    with pytest.raises(RuntimeError) as exc:
        sandbox.run_handler("no_task", (), {}, timeout=1)
    assert "KeyError" in str(exc.value)


def test_timeout_raises_timeout_error(monkeypatch):
    monkeypatch.setattr("asyncmq.conf.settings.sandbox_ctx", "spawn")

    @task(queue="test")
    def block():
        time.sleep(0.5)  # Sleep longer than timeout

    task_id = [k for k, v in TASK_REGISTRY.items() if v["func"] == block][0]

    with pytest.raises(TimeoutError):
        sandbox.run_handler(task_id, [], {}, timeout=0.1, fallback=False)
