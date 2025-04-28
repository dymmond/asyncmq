import asyncio

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.job import Job
from asyncmq.runner import run_worker
from asyncmq.tasks import TASK_REGISTRY, task

pytestmark = pytest.mark.anyio

def get_task_id(func):
    for key, entry in TASK_REGISTRY.items():
        if entry["func"] == func:
            return key
    raise RuntimeError(f"Task {func.__name__} is not registered.")

# Define the tasks
@task(queue="runner")
async def task_a():
    return "task_a completed"

@task(queue="runner")
async def task_b():
    return "task_b completed"

@task(queue="runner")
async def task_c():
    return "task_c completed"


async def test_job_dependencies():
    backend = InMemoryBackend()

    # Create jobs in sequence, task_b depends on task_a, task_c depends on task_b
    job_a = Job(task_id=get_task_id(task_a), args=[], kwargs={})
    job_b = Job(task_id=get_task_id(task_b), args=[], kwargs={})
    job_b.depends_on = [job_a.id]

    job_c = Job(task_id=get_task_id(task_c), args=[], kwargs={})
    job_c.depends_on = [job_b.id]

    # Enqueue jobs with dependencies
    await backend.enqueue("runner", job_a.to_dict())
    await backend.enqueue("runner", job_b.to_dict())  # job_b depends on job_a
    await backend.enqueue("runner", job_c.to_dict())  # job_c depends on job_b

    # Run the worker to process jobs
    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(2)  # Allow enough time for all jobs to finish
    worker.cancel()

    # Verify the result of all jobs
    result_a = await backend.get_job_result("runner", job_a.id)
    result_b = await backend.get_job_result("runner", job_b.id)
    result_c = await backend.get_job_result("runner", job_c.id)

    # Ensure that jobs are processed in the correct order
    assert result_a == "task_a completed"
    assert result_b == "task_b completed"
    assert result_c == "task_c completed"



async def test_independent_jobs():
    backend = InMemoryBackend()

    # Create three independent jobs (no dependencies)
    job_a = Job(task_id=get_task_id(task_a), args=[], kwargs={})
    job_b = Job(task_id=get_task_id(task_b), args=[], kwargs={})
    job_c = Job(task_id=get_task_id(task_c), args=[], kwargs={})

    # Enqueue them
    await backend.enqueue("runner", job_a.to_dict())
    await backend.enqueue("runner", job_b.to_dict())
    await backend.enqueue("runner", job_c.to_dict())

    # Run the worker
    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(2)
    worker.cancel()

    # All three should complete, order doesn't matter
    results = {
        await backend.get_job_result("runner", job_a.id),
        await backend.get_job_result("runner", job_b.id),
        await backend.get_job_result("runner", job_c.id),
    }
    assert results == {"task_a completed", "task_b completed", "task_c completed"}



async def test_multiple_dependencies():
    backend = InMemoryBackend()

    # A and B run first; C depends on both A and B
    job_a = Job(task_id=get_task_id(task_a), args=[], kwargs={})
    job_b = Job(task_id=get_task_id(task_b), args=[], kwargs={})
    job_c = Job(
        task_id=get_task_id(task_c),
        args=[],
        kwargs={},
        depends_on=[job_a.id, job_b.id]
    )

    await backend.enqueue("runner", job_a.to_dict())
    await backend.enqueue("runner", job_b.to_dict())
    await backend.enqueue("runner", job_c.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend))
    await asyncio.sleep(2)
    worker.cancel()

    assert await backend.get_job_result("runner", job_a.id) == "task_a completed"
    assert await backend.get_job_result("runner", job_b.id) == "task_b completed"
    assert await backend.get_job_result("runner", job_c.id) == "task_c completed"


def test_get_task_id_unknown():
    # Passing a function not decorated with @task should raise
    def not_a_registered_task():
        return "nope"

    with pytest.raises(RuntimeError) as exc:
        get_task_id(not_a_registered_task)
    assert "is not registered" in str(exc.value)
