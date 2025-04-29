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
async def hello():
    return "hi"


@task(queue="runner")
async def raise_error():
    raise RuntimeError("fail")


@task(queue="runner")
async def echo(value):
    return value


@task(queue="runner")
async def add(x, y):
    return x + y


@task(queue="runner")
async def fail_once_then_succeed():
    if not hasattr(fail_once_then_succeed, "called"):
        fail_once_then_succeed.called = True
        raise RuntimeError("fail once")
    return "success"


async def test_run_worker_success():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(hello), args=[], kwargs={})
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    result = await backend.get_job_result("runner", job.id)
    state = await backend.get_job_state("runner", job.id)

    assert state == State.COMPLETED
    assert result == "hi"


async def test_run_worker_failure_goes_to_dlq():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(raise_error), args=[], kwargs={}, max_retries=0)
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    state = await backend.get_job_state("runner", job.id)
    assert state == State.FAILED


async def test_echo_value():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(echo), args=["hello world"], kwargs={})
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    result = await backend.get_job_result("runner", job.id)
    assert result == "hello world"


async def test_addition_task():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(add), args=[3, 4], kwargs={})
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    result = await backend.get_job_result("runner", job.id)
    assert result == 7


async def test_fail_then_succeed():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(fail_once_then_succeed), args=[], kwargs={}, backoff=0.1)
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(1.5)
    worker.cancel()

    result = await backend.get_job_result("runner", job.id)
    state = await backend.get_job_state("runner", job.id)
    assert state == State.COMPLETED
    assert result == "success"


async def test_worker_cancels_gracefully():
    backend = InMemoryBackend()
    worker = asyncio.create_task(run_worker("runner", backend=backend))

    worker.cancel()
    await asyncio.sleep(0.3)

    assert worker.cancelled() or worker.done()


async def test_job_ttl_expires():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(hello), args=[], kwargs={}, ttl=0.1)
    await backend.enqueue("runner", job.to_dict())

    # Wait for job to expire before worker starts
    await asyncio.sleep(0.2)

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.3)
    worker.cancel()

    state = await backend.get_job_state("runner", job.id)
    assert state in {State.EXPIRED, State.FAILED}


async def test_worker_handles_multiple_jobs():
    backend = InMemoryBackend()

    job_ids = []
    for i in range(5):
        job = Job(task_id=get_task_id(echo), args=[f"msg-{i}"], kwargs={})
        await backend.enqueue("runner", job.to_dict())
        job_ids.append(job.id)

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(2)
    worker.cancel()

    for job_id in job_ids:
        state = await backend.get_job_state("runner", job_id)
        assert state == State.COMPLETED


async def test_worker_respects_backoff():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(raise_error), args=[], kwargs={}, max_retries=2, backoff=0.2)
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(2)
    worker.cancel()

    state = await backend.get_job_state("runner", job.id)
    assert state == State.FAILED


async def test_worker_skips_expired_jobs():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(hello), args=[], kwargs={}, ttl=0.1)
    await backend.enqueue("runner", job.to_dict())
    await asyncio.sleep(0.3)

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    state = await backend.get_job_state("runner", job.id)
    assert state == State.FAILED or state == State.EXPIRED


async def test_worker_can_recover_from_job_exception():
    backend = InMemoryBackend()
    job1 = Job(task_id=get_task_id(raise_error), args=[], kwargs={}, max_retries=0)
    job2 = Job(task_id=get_task_id(hello), args=[], kwargs={})
    await backend.enqueue("runner", job1.to_dict())
    await backend.enqueue("runner", job2.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(1)
    worker.cancel()

    state1 = await backend.get_job_state("runner", job1.id)
    state2 = await backend.get_job_state("runner", job2.id)
    assert state1 == State.FAILED
    assert state2 == State.COMPLETED


async def test_worker_handles_zero_args():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(hello), args=[], kwargs={})
    await backend.enqueue("runner", job.to_dict())
    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()
    result = await backend.get_job_result("runner", job.id)
    assert result == "hi"


async def test_worker_handles_kwargs():
    @task(queue="runner")
    async def greet(name="world"):
        return f"Hello {name}"

    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(greet), args=[], kwargs={"name": "Tarsil"})
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    result = await backend.get_job_result("runner", job.id)
    assert result == "Hello Tarsil"


async def test_worker_long_chain_jobs():
    backend = InMemoryBackend()
    # Enqueue 10 jobs and track their IDs
    job_ids = []
    for i in range(10):
        job = Job(task_id=get_task_id(echo), args=[f"chain-{i}"], kwargs={})
        await backend.enqueue("runner", job.to_dict())
        job_ids.append(job.id)

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(3)
    worker.cancel()

    for job_id in job_ids:
        state = await backend.get_job_state("runner", job_id)
        assert state == State.COMPLETED
