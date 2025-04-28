import asyncio

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.enums import State
from asyncmq.tasks import task
from asyncmq.worker import handle_job

pytestmark = pytest.mark.anyio

async def wait_for_state(backend, queue, job_id, target, timeout=3):
    for _ in range(int(timeout / 0.1)):
        state = await backend.get_job_state(queue, job_id)
        if state == target:
            return True
        await asyncio.sleep(0.1)
    return False

@task(queue="test")
async def simple_task(x, y):
    return x + y


async def test_job_lifecycle_changes():
    backend = InMemoryBackend()
    await simple_task.enqueue(backend, 3, 4)

    raw = await backend.dequeue("test")
    await handle_job("test", raw, backend=backend,)

    await wait_for_state(backend, "test", raw["id"], State.COMPLETED)
    state = await backend.get_job_state("test", raw["id"])
    result = await backend.get_job_result("test", raw["id"])

    assert state == State.COMPLETED
    assert result == 7

@task(queue="test")
async def failing_task():
    raise RuntimeError("fail")


async def test_failed_job_goes_to_dlq():
    backend = InMemoryBackend()
    await failing_task.enqueue(backend)

    raw = await backend.dequeue("test")
    await handle_job("test", raw, backend=backend,)

    await wait_for_state(backend, "test", raw["id"], State.FAILED)
    state = await backend.get_job_state("test", raw["id"])
    assert state == State.FAILED

@task(queue="test")
async def echo_task(value):
    return value


async def test_echo_result():
    backend = InMemoryBackend()
    await echo_task.enqueue(backend, "hello")
    raw = await backend.dequeue("test")
    await handle_job("test", raw, backend=backend,)
    await wait_for_state(backend, "test", raw["id"], State.COMPLETED)
    result = await backend.get_job_result("test", raw["id"])
    assert result == "hello"

@task(queue="test")
async def sum_list(lst):
    return sum(lst)


async def test_sum_list_task():
    backend = InMemoryBackend()
    await sum_list.enqueue(backend, [1, 2, 3, 4])
    raw = await backend.dequeue("test")
    await handle_job("test", raw, backend=backend,)
    await wait_for_state(backend, "test", raw["id"], State.COMPLETED)
    result = await backend.get_job_result("test", raw["id"])
    assert result == 10

@task(queue="test")
async def upper_case(text):
    return text.upper()


async def test_upper_case():
    backend = InMemoryBackend()
    await upper_case.enqueue(backend, "test")
    raw = await backend.dequeue("test")
    await handle_job("test", raw, backend=backend,)
    await wait_for_state(backend, "test", raw["id"], State.COMPLETED)
    result = await backend.get_job_result("test", raw["id"])
    assert result == "TEST"

@task(queue="test")
async def multiply(a, b):
    return a * b


async def test_multiply_task():
    backend = InMemoryBackend()
    await multiply.enqueue(backend, 6, 7)
    raw = await backend.dequeue("test")
    await handle_job("test", raw, backend=backend)
    await wait_for_state(backend, "test", raw["id"], State.COMPLETED)
    result = await backend.get_job_result("test", raw["id"])
    assert result == 42
