import asyncio
import time

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.core.enums import State
from asyncmq.jobs import Job

pytestmark = pytest.mark.anyio


async def test_enqueue_and_dequeue():
    backend = InMemoryBackend()
    job = Job(task_id="test.task", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    result = await backend.dequeue("test")
    assert result["id"] == job.id


async def test_job_state_tracking():
    backend = InMemoryBackend()
    job = Job(task_id="state.test", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    await backend.update_job_state("test", job.id, State.ACTIVE)
    state = await backend.get_job_state("test", job.id)
    assert state == State.ACTIVE


async def test_save_and_get_job_result():
    backend = InMemoryBackend()
    job = Job(task_id="result.test", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    await backend.save_job_result("test", job.id, 1234)
    result = await backend.get_job_result("test", job.id)
    assert result == 1234


async def test_enqueue_delayed_and_get_due():
    backend = InMemoryBackend()
    job = Job(task_id="delay.test", args=[], kwargs={})
    run_at = time.time() + 0.2
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    await asyncio.sleep(0.25)
    due = await backend.get_due_delayed("test")
    assert any(j["id"] == job.id for j in due)


async def test_move_to_dlq():
    backend = InMemoryBackend()
    job = Job(task_id="dlq.test", args=[], kwargs={})
    await backend.move_to_dlq("test", job.to_dict())
    # no direct getter, so we check state
    state = await backend.get_job_state("test", job.id)
    assert state == State.FAILED


async def test_remove_delayed():
    backend = InMemoryBackend()
    job = Job(task_id="delay.remove", args=[], kwargs={})
    run_at = time.time() + 1
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    await backend.remove_delayed("test", job.id)
    delayed = await backend.get_due_delayed("test")
    assert all(j["id"] != job.id for j in delayed)


@pytest.mark.parametrize("state", ["waiting", "delayed", "failed"])
async def test_list_jobs_by_state(state):
    backend = InMemoryBackend()
    queue = "test-queue"
    job = Job(task_id="test.task", args=[], kwargs={})

    if state == "waiting":
        await backend.enqueue(queue, job.to_dict())
    elif state == "delayed":
        delayed_job = job.to_dict()
        await backend.enqueue_delayed(queue, delayed_job, run_at=9999999999)
    elif state == "failed":
        await backend.enqueue(queue, job.to_dict())
        await backend.move_to_dlq(queue, job.to_dict())

    jobs = await backend.list_jobs(queue, state)
    print("Returned jobs:", jobs)
    assert isinstance(jobs, list)
    assert any(j.get("task") == "test.task" for j in jobs)


@pytest.mark.parametrize("state", ["waiting", "delayed", "failed"])
async def test_list_jobs_empty_queue(state):
    backend = InMemoryBackend()
    jobs = await backend.list_jobs("empty-queue", state)
    assert isinstance(jobs, list)
    assert len(jobs) == 0


async def test_list_jobs_filters_correctly():
    backend = InMemoryBackend()
    queue = "filter-test"

    job1 = Job(task_id="waiting.job", args=[], kwargs={})
    await backend.enqueue(queue, job1.to_dict())

    job2 = Job(task_id="delayed.job", args=[], kwargs={})
    await backend.enqueue_delayed(queue, job2.to_dict(), run_at=9999999999)

    job3 = Job(task_id="failed.job", args=[], kwargs={})
    await backend.move_to_dlq(queue, job3.to_dict())

    waiting = await backend.list_jobs(queue, "waiting")
    delayed = await backend.list_jobs(queue, "delayed")
    failed = await backend.list_jobs(queue, "failed")

    assert any(j["task"] == "waiting.job" for j in waiting)
    assert all(j["task"] == "delayed.job" for j in delayed)
    assert all(j["task"] == "failed.job" for j in failed)
