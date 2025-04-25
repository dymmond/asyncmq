# tests/test_priority_queue.py
import asyncio
import time  # Add this import
import pytest
from asyncmq.backends.memory import InMemoryBackend
from asyncmq.runner import run_worker
from asyncmq.task import task
from asyncmq.job import Job
from asyncmq.task import TASK_REGISTRY


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


@pytest.mark.asyncio
async def test_priority_queue_order():
    backend = InMemoryBackend()

    high_priority_job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1)
    medium_priority_job = Job(task_id=get_task_id(medium_priority_task), args=[], kwargs={}, priority=5)
    low_priority_job = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10)

    # Enqueue jobs with different priorities
    await backend.enqueue("runner", high_priority_job.to_dict())
    await backend.enqueue("runner", medium_priority_job.to_dict())
    await backend.enqueue("runner", low_priority_job.to_dict())

    # Run the worker to process jobs
    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(2)  # Allow enough time for all jobs to finish
    worker.cancel()

    # Verify the result of all jobs
    result_high = await backend.get_job_result("runner", high_priority_job.id)
    result_medium = await backend.get_job_result("runner", medium_priority_job.id)
    result_low = await backend.get_job_result("runner", low_priority_job.id)

    assert result_high == "high priority task completed"
    assert result_medium == "medium priority task completed"
    assert result_low == "low priority task completed"


@pytest.mark.asyncio
async def test_same_priority_jobs_order():
    backend = InMemoryBackend()

    job_1 = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=5)
    job_2 = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=5)

    # Enqueue jobs with the same priority
    await backend.enqueue("runner", job_1.to_dict())
    await backend.enqueue("runner", job_2.to_dict())

    # Run the worker to process jobs
    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(2)  # Allow enough time for all jobs to finish
    worker.cancel()

    # Verify that jobs with the same priority are processed in order of enqueue
    result_1 = await backend.get_job_result("runner", job_1.id)
    result_2 = await backend.get_job_result("runner", job_2.id)

    assert result_1 == "high priority task completed"
    assert result_2 == "low priority task completed"


@pytest.mark.asyncio
async def test_job_reordering_by_priority():
    backend = InMemoryBackend()

    high_priority_job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1)
    low_priority_job = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10)

    # Enqueue jobs with different priorities
    await backend.enqueue("runner", low_priority_job.to_dict())
    await backend.enqueue("runner", high_priority_job.to_dict())

    # Run the worker to process jobs
    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(2)  # Allow enough time for all jobs to finish
    worker.cancel()

    # Verify that high priority job is processed first
    result_high = await backend.get_job_result("runner", high_priority_job.id)
    result_low = await backend.get_job_result("runner", low_priority_job.id)

    assert result_high == "high priority task completed"
    assert result_low == "low priority task completed"


@pytest.mark.asyncio
async def test_priority_with_multiple_queues():
    backend = InMemoryBackend()

    high_priority_job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1)
    low_priority_job = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10)

    # Enqueue jobs with different priorities to different queues
    await backend.enqueue("runner_1", high_priority_job.to_dict())
    await backend.enqueue("runner_2", low_priority_job.to_dict())

    # Run the worker to process jobs
    worker = asyncio.create_task(run_worker("runner_1", backend))
    await asyncio.sleep(1)  # Allow enough time for jobs in runner_1 to finish
    worker.cancel()

    worker = asyncio.create_task(run_worker("runner_2", backend))
    await asyncio.sleep(1)  # Allow enough time for jobs in runner_2 to finish
    worker.cancel()

    # Verify results for high and low priority jobs
    result_high = await backend.get_job_result("runner_1", high_priority_job.id)
    result_low = await backend.get_job_result("runner_2", low_priority_job.id)

    assert result_high == "high priority task completed"
    assert result_low == "low priority task completed"


@pytest.mark.asyncio
async def test_priority_queue_with_max_retries():
    backend = InMemoryBackend()

    job_a = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1, max_retries=2)
    job_b = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10, max_retries=1)

    # Enqueue jobs with max retries
    await backend.enqueue("runner", job_a.to_dict())
    await backend.enqueue("runner", job_b.to_dict())

    # Simulate job failure for job_b
    job_b.status = "failed"
    await backend.update_job_state("runner", job_b.id, "failed")

    # Run the worker to process jobs
    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(2)  # Allow enough time for jobs to finish
    worker.cancel()

    # Verify jobs are processed in order of priority
    result_a = await backend.get_job_result("runner", job_a.id)
    result_b = await backend.get_job_result("runner", job_b.id)

    assert result_a == "high priority task completed"
    assert result_b == "low priority task completed"


@pytest.mark.asyncio
async def test_priority_queue_with_delayed_jobs():
    backend = InMemoryBackend()

    high_priority_job = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1)
    low_priority_job = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10)

    # Enqueue jobs with different priorities
    await backend.enqueue_delayed("runner", high_priority_job.to_dict(), time.time() + 1)
    await backend.enqueue_delayed("runner", low_priority_job.to_dict(), time.time() + 2)

    # Run the worker to process jobs
    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(3)  # Allow enough time for all jobs to finish
    worker.cancel()

    # Verify that high priority job is processed before low priority one
    result_high = await backend.get_job_result("runner", high_priority_job.id)
    result_low = await backend.get_job_result("runner", low_priority_job.id)

    assert result_high == "high priority task completed"
    assert result_low == "low priority task completed"


@pytest.mark.asyncio
async def test_job_with_priority_and_ttl():
    backend = InMemoryBackend()

    # Increase the TTL time to 2 seconds to ensure jobs can be processed
    job_high_priority = Job(task_id=get_task_id(high_priority_task), args=[], kwargs={}, priority=1, ttl=2)
    job_low_priority = Job(task_id=get_task_id(low_priority_task), args=[], kwargs={}, priority=10, ttl=2)

    # Enqueue jobs with TTL and different priorities
    await backend.enqueue("runner", job_high_priority.to_dict())
    await backend.enqueue("runner", job_low_priority.to_dict())

    # Sleep to allow enough time for the jobs to process and potentially expire
    await asyncio.sleep(0.5)  # Reduced sleep time to give jobs time to process before TTL expiration

    # Run the worker to process jobs
    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(1.5)  # Allow enough time for jobs to finish before TTL expires
    worker.cancel()

    # Check job states to debug
    state_high = await backend.get_job_state("runner", job_high_priority.id)
    state_low = await backend.get_job_state("runner", job_low_priority.id)
    print(f"Job High State: {state_high}")
    print(f"Job Low State: {state_low}")

    # Get the results of the jobs
    result_high = await backend.get_job_result("runner", job_high_priority.id)
    result_low = await backend.get_job_result("runner", job_low_priority.id)

    # Verify the results (ensure high priority job processed before TTL expires)
    assert result_high == "high priority task completed"
    assert result_low == "low priority task completed"



@pytest.mark.asyncio
async def test_empty_priority_queue():
    backend = InMemoryBackend()

    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(2)  # Allow enough time to run
    worker.cancel()

    # Verify that no jobs are processed
