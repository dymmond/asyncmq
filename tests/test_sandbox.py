import time

import pytest

from asyncmq import sandbox
from asyncmq.tasks import TASK_REGISTRY


def register_task(task_id, func):
    """
    Register a task in TASK_REGISTRY for sandbox tests.
    """
    TASK_REGISTRY[task_id] = {"func": func}

@pytest.fixture(autouse=True)
def clear_registry():
    # Save original registry entries and clear
    original = dict(TASK_REGISTRY)
    TASK_REGISTRY.clear()
    yield
    # Restore original
    TASK_REGISTRY.clear()
    TASK_REGISTRY.update(original)


def test_success_handler_returns_value():
    def handler(x, y):
        return x + y
    register_task('add', handler)
    result = sandbox.run_handler('add', (2, 3), {}, timeout=5)
    assert result == 5


def test_async_handler_returns_value():
    async def handler(x):
        # simulate async work
        import anyio
        await anyio.sleep(0.01)
        return x * 2
    register_task('double', handler)
    result = sandbox.run_handler('double', (4,), {}, timeout=5)
    assert result == 8


def test_handler_exception_propagated():
    def handler():
        raise ValueError("oops")
    register_task('boom', handler)
    with pytest.raises(RuntimeError) as excinfo:
        sandbox.run_handler('boom', (), {}, timeout=5)
    msg = str(excinfo.value)
    assert 'ValueError' in msg
    assert 'oops' in msg


def test_timeout_raises_timeout_error():
    def handler():
        # Block longer than timeout
        time.sleep(0.1)
    register_task('sleep', handler)
    with pytest.raises(TimeoutError):
        sandbox.run_handler('sleep', (), {}, timeout=0.01)


def test_missing_task_raises_key_error():
    # Ensure TASK_REGISTRY has no entry for this id
    TASK_REGISTRY.pop('no_task', None)
    with pytest.raises(KeyError):
        sandbox.run_handler('no_task', (), {}, timeout=1)
