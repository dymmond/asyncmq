import asyncio

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.core.enums import State
from asyncmq.jobs import Job
from asyncmq.runners import run_worker
from asyncmq.tasks import TASK_REGISTRY, task

pytestmark = pytest.mark.anyio


def get_task_id(func):
    for key, entry in TASK_REGISTRY.items():
        if entry["func"] == func:
            return key
    raise RuntimeError(f"Task {func.__name__} is not registered.")


@task(queue="runner")
async def high_priority_task():
    return "high priority task completed"


@task(queue="runner")
async def low_priority_task():
    return "low priority task completed"


@task(queue="runner")
async def medium_priority_task():
    return "medium priority task completed"


@task(queue="runner")
async def raise_error():
    raise RuntimeError("fail")


async def test_rate_limited_task_execution():
    backend = InMemoryBackend()

    high_priority_job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1)
    low_priority_job = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10)
    medium_priority_job = Job(task_id=get_task_id(medium_priority_task), args=[], kwargs={}, priority=5)

    await backend.enqueue("runner", high_priority_job.to_dict())
    await backend.enqueue("runner", low_priority_job.to_dict())
    await backend.enqueue("runner", medium_priority_job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend, concurrency=3, rate_limit=3, rate_interval=1))
    await asyncio.sleep(2)  # Let the rate-limiting occur
    worker.cancel()

    # Check the result of the high priority job
    result_high = await backend.get_job_result("runner", high_priority_job.id)
    result_medium = await backend.get_job_result("runner", medium_priority_job.id)
    result_low = await backend.get_job_result("runner", low_priority_job.id)

    assert result_high == "high priority task completed"
    assert result_medium == "medium priority task completed"
    assert result_low == "low priority task completed"


async def test_rate_limit_with_multiple_workers():
    backend = InMemoryBackend()

    high_priority_job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1)
    low_priority_job = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10)

    await backend.enqueue("runner", high_priority_job.to_dict())
    await backend.enqueue("runner", low_priority_job.to_dict())

    worker_1 = asyncio.create_task(run_worker("runner", backend=backend, concurrency=3, rate_limit=3, rate_interval=1))
    worker_2 = asyncio.create_task(run_worker("runner", backend=backend, concurrency=3, rate_limit=3, rate_interval=1))
    await asyncio.sleep(2)  # Let the rate-limiting occur
    worker_1.cancel()
    worker_2.cancel()

    # Check the result of the high priority job
    result_high = await backend.get_job_result("runner", high_priority_job.id)
    result_low = await backend.get_job_result("runner", low_priority_job.id)

    assert result_high == "high priority task completed"
    assert result_low == "low priority task completed"


async def test_rate_limit_with_task_retries():
    backend = InMemoryBackend()

    job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1, max_retries=2)

    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend, concurrency=3, rate_limit=3, rate_interval=1))
    await asyncio.sleep(2)  # Let the rate-limiting occur
    worker.cancel()

    # Check if the job has been retried correctly after rate-limiting
    result = await backend.get_job_result("runner", job.id)
    assert result == "high priority task completed"


async def test_rate_limit_with_job_failure():
    backend = InMemoryBackend()

    job = Job(task_id=get_task_id(raise_error), args=[], kwargs={}, priority=1, max_retries=1, backoff=0)

    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend, concurrency=3, rate_limit=3, rate_interval=1))
    await asyncio.sleep(2)  # Let the rate-limiting occur
    worker.cancel()

    # Check if the job is moved to the DLQ after failure
    state = await backend.get_job_state("runner", job.id)
    assert state in {State.FAILED, State.EXPIRED}  # Depending on scan timing, job may still be delayed or failed


async def test_rate_limit_with_multiple_jobs_in_one_period():
    backend = InMemoryBackend()

    # Enqueue 5 identical jobs
    jobs = [Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1) for _ in range(5)]
    for job in jobs:
        await backend.enqueue("runner", job.to_dict())

    # Start worker with rate=3 per second
    worker = asyncio.create_task(run_worker("runner", backend=backend, concurrency=3, rate_limit=3, rate_interval=1))
    await asyncio.sleep(0.2)  # < 1 second, so only 3 tokens are available
    worker.cancel()

    # First 3 jobs should have completed
    for job in jobs[:3]:
        result = await backend.get_job_result("runner", job.id)
        assert result == "high priority task completed"
    # Last 2 should still be waiting for tokens
    for job in jobs[3:]:
        state = await backend.get_job_state("runner", job.id)
        assert state == State.WAITING
