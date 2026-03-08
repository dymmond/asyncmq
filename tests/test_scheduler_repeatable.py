import asyncio
import time

import pytest

from asyncmq import monkay
from asyncmq.backends.memory import InMemoryBackend
from asyncmq.core.enums import State
from asyncmq.queues import Queue
from asyncmq.runners import run_worker
from asyncmq.schedulers import repeatable_scheduler
from asyncmq.tasks import TASK_REGISTRY, task
from asyncmq.workers import handle_job

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def disable_sandbox_for_repeatable_tests():
    previous = monkay.settings.sandbox_enabled
    monkay.settings.sandbox_enabled = False
    try:
        yield
    finally:
        monkay.settings.sandbox_enabled = previous


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
            await handle_job(queue, raw, backend)
        await asyncio.sleep(0.05)


@task(queue="repeatable")
async def repeat_me():
    repeat_me.counter += 1
    return repeat_me.counter


repeat_me.counter = 0


async def test_repeatable_scheduler_triggers():
    backend = InMemoryBackend()
    jobs = [{"task_id": get_task_id(repeat_me), "args": [], "kwargs": {}, "every": 1.0, "queue": "repeatable"}]

    scheduler = asyncio.create_task(repeatable_scheduler("repeatable", jobs, backend=backend, interval=0.5))
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


async def test_repeatable_multiple_tasks():
    backend = InMemoryBackend()
    jobs = [
        {"task_id": get_task_id(ping), "args": [], "kwargs": {}, "every": 1, "queue": "repeatable"},
        {"task_id": get_task_id(print_hello), "args": [], "kwargs": {}, "every": 1.5, "queue": "repeatable"},
        {"task_id": get_task_id(inc), "args": [], "kwargs": {}, "every": 2, "queue": "repeatable"},
    ]
    scheduler = asyncio.create_task(repeatable_scheduler("repeatable", jobs, backend=backend, interval=0.5))
    worker = asyncio.create_task(handle_all_jobs(backend, "repeatable"))
    await asyncio.sleep(4)
    scheduler.cancel()
    worker.cancel()

    assert ping.counter >= 2
    assert print_hello.counter >= 2
    assert inc.counter >= 2


async def test_repeatable_args_and_kwargs():
    backend = InMemoryBackend()
    jobs = [{"task_id": get_task_id(echo_repeat), "args": ["data"], "kwargs": {}, "every": 1, "queue": "repeatable"}]
    scheduler = asyncio.create_task(repeatable_scheduler("repeatable", jobs, backend=backend, interval=0.5))
    await asyncio.sleep(2.5)
    scheduler.cancel()
    dequeued = await backend.dequeue("repeatable")
    assert dequeued["args"] == ["data"]


async def test_repeatable_noop():
    backend = InMemoryBackend()
    jobs = [{"task_id": get_task_id(noop), "args": [], "kwargs": {}, "every": 1, "queue": "repeatable"}]
    scheduler = asyncio.create_task(repeatable_scheduler("repeatable", jobs, backend=backend, interval=0.2))
    await asyncio.sleep(1.5)
    scheduler.cancel()
    job = await backend.dequeue("repeatable")
    assert job["task"] == get_task_id(noop)


async def test_repeatable_job_status():
    backend = InMemoryBackend()
    jobs = [{"task_id": get_task_id(ping), "args": [], "kwargs": {}, "every": 1, "queue": "repeatable"}]
    scheduler = asyncio.create_task(repeatable_scheduler("repeatable", jobs, backend=backend, interval=0.3))
    await asyncio.sleep(2)
    scheduler.cancel()
    raw = await backend.dequeue("repeatable")
    assert raw["status"] == State.WAITING


async def test_repeatable_job_interval_variation():
    backend = InMemoryBackend()
    jobs = [{"task_id": get_task_id(echo_repeat), "args": ["x"], "kwargs": {}, "every": 0.7, "queue": "repeatable"}]
    scheduler = asyncio.create_task(repeatable_scheduler("repeatable", jobs, backend=backend, interval=0.1))
    await asyncio.sleep(1.6)
    scheduler.cancel()
    count = 0
    while await backend.dequeue("repeatable"):
        count += 1
    assert count >= 2


@task(queue="repeatable")
async def durable_ping():
    durable_ping.counter += 1
    return "durable-pong"


durable_ping.counter = 0


async def test_repeatable_scheduler_picks_up_backend_registered_repeatables():
    backend = InMemoryBackend()
    queue = Queue("repeatable", backend=backend)
    durable_ping.counter = 0

    await queue.upsert_repeatable(task_id=get_task_id(durable_ping), every=0.25)

    scheduler = asyncio.create_task(repeatable_scheduler("repeatable", [], backend=backend, interval=0.05))
    worker = asyncio.create_task(handle_all_jobs(backend, "repeatable"))
    await asyncio.sleep(0.9)
    scheduler.cancel()
    worker.cancel()

    assert durable_ping.counter >= 2


async def test_run_worker_processes_backend_repeatables_without_local_repeatables():
    backend = InMemoryBackend()
    queue = Queue("repeatable", backend=backend)
    durable_ping.counter = 0

    await queue.upsert_repeatable(task_id=get_task_id(durable_ping), every=0.2)

    worker = asyncio.create_task(
        run_worker("repeatable", backend=backend, concurrency=1, rate_limit=None, rate_interval=1.0, repeatables=None)
    )
    await asyncio.sleep(0.75)
    worker.cancel()

    assert durable_ping.counter >= 2


async def test_repeatable_scheduler_preserves_sandbox_setting():
    backend = InMemoryBackend()
    previous = monkay.settings.sandbox_enabled
    monkay.settings.sandbox_enabled = True
    try:
        scheduler = asyncio.create_task(repeatable_scheduler("repeatable", [], backend=backend, interval=0.05))
        await asyncio.sleep(0.12)
        scheduler.cancel()
        with pytest.raises(asyncio.CancelledError):
            await scheduler

        assert monkay.settings.sandbox_enabled is True
    finally:
        monkay.settings.sandbox_enabled = previous


async def test_repeatable_scheduler_coordinates_backend_repeatables_with_lock():
    backend = InMemoryBackend()
    queue = Queue("repeatable", backend=backend)

    await queue.upsert_repeatable(task_id=get_task_id(durable_ping), every=3600)
    repeatable_id = next(iter(backend.repeatables["repeatable"]))
    backend.repeatables["repeatable"][repeatable_id]["next_run"] = time.time() - 0.01

    scheduler_one = asyncio.create_task(repeatable_scheduler("repeatable", [], backend=backend, interval=0.05))
    scheduler_two = asyncio.create_task(repeatable_scheduler("repeatable", [], backend=backend, interval=0.05))
    await asyncio.sleep(0.15)
    scheduler_one.cancel()
    scheduler_two.cancel()

    for scheduler in (scheduler_one, scheduler_two):
        with pytest.raises(asyncio.CancelledError):
            await scheduler

    waiting = await queue.get_waiting(asc=True)
    assert len(waiting) == 1
