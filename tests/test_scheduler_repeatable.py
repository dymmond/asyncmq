import asyncio

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.scheduler import repeatable_scheduler
from asyncmq.task import TASK_REGISTRY, task
from asyncmq.worker import handle_job


# Helper to fetch task IDs dynamically
def get_task_id(func):
    for k, v in TASK_REGISTRY.items():
        if v["func"] == func:
            return k
    raise ValueError(f"Task {func.__name__} not registered.")

async def handle_all_jobs(backend, queue):
    while True:
        raw = await backend.dequeue(queue)
        if raw:
            await handle_job(queue, backend, raw)
        await asyncio.sleep(0.05)

@task(queue="repeatable")
async def repeat_me():
    repeat_me.counter += 1
    return repeat_me.counter
repeat_me.counter = 0

@pytest.mark.asyncio
async def test_repeatable_scheduler_triggers():
    backend = InMemoryBackend()
    jobs = [{
        "task_id": get_task_id(repeat_me),
        "args": [],
        "kwargs": {},
        "repeat_every": 1.0,
        "queue": "repeatable"
    }]

    scheduler = asyncio.create_task(
        repeatable_scheduler(backend, "repeatable", jobs, interval=0.5)
    )
    await asyncio.sleep(3.2)
    scheduler.cancel()

    count = 0
    while await backend.dequeue("repeatable"):
        count += 1
    assert count >= 3

@task(queue="repeatable")
async def ping():
    ping.counter += 1
    return "pong"
ping.counter = 0

@task(queue="repeatable")
async def print_hello():
    print_hello.counter += 1
    return "hello"
print_hello.counter = 0

@task(queue="repeatable")
async def inc():
    inc.counter += 2
    return inc.counter
inc.counter = 0

@task(queue="repeatable")
async def echo_repeat(val):
    return val

@task(queue="repeatable")
async def noop():
    pass

@pytest.mark.asyncio
async def test_repeatable_multiple_tasks():
    backend = InMemoryBackend()
    jobs = [
        {"task_id": get_task_id(ping), "args": [], "kwargs": {}, "repeat_every": 1, "queue": "repeatable"},
        {"task_id": get_task_id(print_hello), "args": [], "kwargs": {}, "repeat_every": 1.5, "queue": "repeatable"},
        {"task_id": get_task_id(inc), "args": [], "kwargs": {}, "repeat_every": 2, "queue": "repeatable"},
    ]
    scheduler = asyncio.create_task(repeatable_scheduler(backend, "repeatable", jobs, interval=0.5))
    worker = asyncio.create_task(handle_all_jobs(backend, "repeatable"))
    await asyncio.sleep(4)
    scheduler.cancel()
    worker.cancel()

    assert ping.counter >= 2
    assert print_hello.counter >= 2
    assert inc.counter >= 2

@pytest.mark.asyncio
async def test_repeatable_args_and_kwargs():
    backend = InMemoryBackend()
    jobs = [{
        "task_id": get_task_id(echo_repeat),
        "args": ["data"],
        "kwargs": {},
        "repeat_every": 1,
        "queue": "repeatable"
    }]
    scheduler = asyncio.create_task(repeatable_scheduler(backend, "repeatable", jobs, interval=0.5))
    await asyncio.sleep(2.5)
    scheduler.cancel()
    dequeued = await backend.dequeue("repeatable")
    assert dequeued["args"] == ["data"]

@pytest.mark.asyncio
async def test_repeatable_noop():
    backend = InMemoryBackend()
    jobs = [{
        "task_id": get_task_id(noop),
        "args": [],
        "kwargs": {},
        "repeat_every": 1,
        "queue": "repeatable"
    }]
    scheduler = asyncio.create_task(repeatable_scheduler(backend, "repeatable", jobs, interval=0.2))
    await asyncio.sleep(1.5)
    scheduler.cancel()
    job = await backend.dequeue("repeatable")
    assert job["task"] == get_task_id(noop)

@pytest.mark.asyncio
async def test_repeatable_job_status():
    backend = InMemoryBackend()
    jobs = [{
        "task_id": get_task_id(ping),
        "args": [],
        "kwargs": {},
        "repeat_every": 1,
        "queue": "repeatable"
    }]
    scheduler = asyncio.create_task(repeatable_scheduler(backend, "repeatable", jobs, interval=0.3))
    await asyncio.sleep(2)
    scheduler.cancel()
    raw = await backend.dequeue("repeatable")
    assert raw["status"] == "waiting"

@pytest.mark.asyncio
async def test_repeatable_job_interval_variation():
    backend = InMemoryBackend()
    jobs = [{
        "task_id": get_task_id(echo_repeat),
        "args": ["x"],
        "kwargs": {},
        "repeat_every": 0.7,
        "queue": "repeatable"
    }]
    scheduler = asyncio.create_task(repeatable_scheduler(backend, "repeatable", jobs, interval=0.1))
    await asyncio.sleep(1.6)
    scheduler.cancel()
    count = 0
    while await backend.dequeue("repeatable"):
        count += 1
    assert count >= 2
