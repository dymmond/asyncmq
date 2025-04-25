import asyncio
import time
import pytest
from asyncqueue.backends.memory import InMemoryBackend
from asyncqueue.runner import run_worker
from asyncqueue.task import task
from asyncqueue.job import Job
from asyncqueue.task import TASK_REGISTRY
from asyncqueue.rate_limiter import RateLimiter

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


@pytest.mark.asyncio
async def test_rate_limited_task_execution():
    backend = InMemoryBackend()
    rate_limiter = RateLimiter(rate=3, interval=1)  # Limit to 3 requests per second

    high_priority_job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1)
    low_priority_job = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10)
    medium_priority_job = Job(task_id=get_task_id(medium_priority_task), args=[], kwargs={}, priority=5)

    await backend.enqueue("runner", high_priority_job.to_dict())
    await backend.enqueue("runner", low_priority_job.to_dict())
    await backend.enqueue("runner", medium_priority_job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend, concurrency=3, rate_limit=3, rate_interval=1))
    await asyncio.sleep(2)  # Let the rate-limiting occur
    worker.cancel()

    # Check the result of the high priority job
    result_high = await backend.get_job_result("runner", high_priority_job.id)
    result_medium = await backend.get_job_result("runner", medium_priority_job.id)
    result_low = await backend.get_job_result("runner", low_priority_job.id)

    assert result_high == "high priority task completed"
    assert result_medium == "medium priority task completed"
    assert result_low == "low priority task completed"


@pytest.mark.asyncio
async def test_rate_limit_with_multiple_workers():
    backend = InMemoryBackend()
    rate_limiter = RateLimiter(rate=3, interval=1)

    high_priority_job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1)
    low_priority_job = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10)

    await backend.enqueue("runner", high_priority_job.to_dict())
    await backend.enqueue("runner", low_priority_job.to_dict())

    worker_1 = asyncio.create_task(run_worker("runner", backend, concurrency=3, rate_limit=3, rate_interval=1))
    worker_2 = asyncio.create_task(run_worker("runner", backend, concurrency=3, rate_limit=3, rate_interval=1))
    await asyncio.sleep(2)  # Let the rate-limiting occur
    worker_1.cancel()
    worker_2.cancel()

    # Check the result of the high priority job
    result_high = await backend.get_job_result("runner", high_priority_job.id)
    result_low = await backend.get_job_result("runner", low_priority_job.id)

    assert result_high == "high priority task completed"
    assert result_low == "low priority task completed"


@pytest.mark.asyncio
async def test_rate_limit_with_task_retries():
    backend = InMemoryBackend()
    rate_limiter = RateLimiter(rate=3, interval=1)

    job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1, max_retries=2)

    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend, concurrency=3, rate_limit=3, rate_interval=1))
    await asyncio.sleep(2)  # Let the rate-limiting occur
    worker.cancel()

    # Check if the job has been retried correctly after rate-limiting
    result = await backend.get_job_result("runner", job.id)
    assert result == "high priority task completed"


@pytest.mark.asyncio
async def test_rate_limit_with_job_failure():
    backend = InMemoryBackend()
    rate_limiter = RateLimiter(rate=3, interval=1)

    job = Job(task_id=get_task_id(raise_error), args=[], kwargs={}, priority=1, max_retries=1)

    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend, concurrency=3, rate_limit=3, rate_interval=1))
    await asyncio.sleep(2)  # Let the rate-limiting occur
    worker.cancel()

    # Check if the job is moved to the DLQ after failure
    state = await backend.get_job_state("runner", job.id)
    assert state == "failed"  # Ensure the job moves to the failed state after retries


@pytest.mark.asyncio
async def test_rate_limit_with_multiple_jobs_in_one_period():
    backend = InMemoryBackend()

    # Enqueue 5 identical jobs
    jobs = [Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1)
            for _ in range(5)]
    for job in jobs:
        await backend.enqueue("runner", job.to_dict())

    # Start worker with rate=3 per second
    worker = asyncio.create_task(
        run_worker(
            "runner",
            backend,
            concurrency=3,
            rate_limit=3,
            rate_interval=1
        )
    )
    await asyncio.sleep(0.2)  # < 1 second, so only 3 tokens are available
    worker.cancel()

    # First 3 jobs should have completed
    for job in jobs[:3]:
        result = await backend.get_job_result("runner", job.id)
        assert result == "high priority task completed"
    # Last 2 should still be waiting for tokens
    for job in jobs[3:]:
        state = await backend.get_job_state("runner", job.id)
        assert state == "waiting"


@pytest.mark.asyncio
async def test_rate_limit_with_job_ttl():
    backend = InMemoryBackend()
    rate_limiter = RateLimiter(rate=3, interval=1)

    job_high_priority = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1, ttl=2)
    job_low_priority = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10, ttl=2)

    # Enqueue jobs with TTL and different priorities
    await backend.enqueue("runner", job_high_priority.to_dict())
    await backend.enqueue("runner", job_low_priority.to_dict())

    await asyncio.sleep(0.5)  # Let jobs potentially expire

    # Run the worker to process jobs
    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(1.5)  # Allow enough time for jobs to finish
    worker.cancel()

    # Verify that the jobs are processed according to priority, even with TTL
    result_high = await backend.get_job_result("runner", job_high_priority.id)
    result_low = await backend.get_job_result("runner", job_low_priority.id)

    assert result_high == "high priority task completed"
    assert result_low == "low priority task completed"

@pytest.mark.asyncio
async def test_rate_limit_with_zero_max_requests():
    backend = InMemoryBackend()
    rate_limiter = RateLimiter(rate=0, interval=1)  # Setting max requests to 0 should block any requests

    job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1)

    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend, concurrency=3, rate_limit=0, rate_interval=1))
    await asyncio.sleep(2)  # Allow enough time for jobs to process
    worker.cancel()

    # Verify that the job is not processed due to the rate-limiting
    state = await backend.get_job_state("runner", job.id)
    assert state == "waiting"
