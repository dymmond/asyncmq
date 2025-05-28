import asyncio
import time

import pytest

from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.jobs import Job

pytestmark = pytest.mark.anyio


@pytest.fixture
async def backend():
    backend = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database="test_asyncmq")
    yield backend
    # Clean up: drop database after tests
    backend.store.client.drop_database("test_asyncmq")


async def test_enqueue_return_job_id(backend):
    job = {"id": "job1", "task_id": "task1", "args": [], "kwargs": {}}
    job_id = await backend.enqueue("test-queue", job)

    assert job_id is not None

async def test_delay_return_job_id(backend):
    job = {"id": "job1", "task_id": "task1", "args": [], "kwargs": {}}
    job_id = await backend.delay("test-queue", job)

    assert job_id is not None


async def test_enqueue_and_dequeue(backend):
    job = {"id": "job1", "task_id": "task1", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job)

    dequeued = await backend.dequeue("test-queue")
    assert dequeued is not None
    assert dequeued["id"] == "job1"


async def test_move_to_dlq(backend):
    job = {"id": "job2", "task_id": "task2", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job)
    await backend.move_to_dlq("test-queue", job)

    loaded = await backend.store.load("test-queue", "job2")
    assert loaded["status"] == "failed"


async def test_enqueue_delayed_and_get_due(backend):
    job = {"id": "job3", "task_id": "task3", "args": [], "kwargs": {}}
    run_at = time.time() + 1
    await backend.enqueue_delayed("test-queue", job, run_at)

    due_jobs = await backend.get_due_delayed("test-queue")
    assert due_jobs == []

    await asyncio.sleep(1.5)
    due_jobs = await backend.get_due_delayed("test-queue")
    assert any(j["id"] == "job3" for j in due_jobs)


async def test_update_and_get_job_state(backend):
    job = {"id": "job4", "task_id": "task4", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job)

    await backend.update_job_state("test-queue", "job4", "completed")
    state = await backend.get_job_state("test-queue", "job4")
    assert state == "completed"


async def test_save_and_get_job_result(backend):
    job = {"id": "job5", "task_id": "task5", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job)

    await backend.save_job_result("test-queue", "job5", {"result": 42})
    result = await backend.get_job_result("test-queue", "job5")
    assert result["result"] == 42


async def test_bulk_enqueue(backend):
    jobs = [
        {"id": "job6", "task_id": "task6", "args": [], "kwargs": {}},
        {"id": "job7", "task_id": "task7", "args": [], "kwargs": {}},
    ]
    await backend.bulk_enqueue("test-queue", jobs)

    dequeued1 = await backend.dequeue("test-queue")
    dequeued2 = await backend.dequeue("test-queue")

    assert {dequeued1["id"], dequeued2["id"]} == {"job6", "job7"}


async def test_pause_resume(backend):
    await backend.pause_queue("test-queue")
    assert await backend.is_queue_paused("test-queue") is True

    await backend.resume_queue("test-queue")
    assert await backend.is_queue_paused("test-queue") is False


@pytest.mark.parametrize("state", ["waiting", "delayed", "failed"])
async def test_list_jobs_by_state(backend, state):
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
async def test_list_jobs_empty_queue(backend, state):
    jobs = await backend.list_jobs("empty-queue", state)
    assert isinstance(jobs, list)
    assert len(jobs) == 0


async def test_list_jobs_filters_correctly(backend):
    queue = "filter-test"
    job1 = Job(task_id="waiting.job", args=[], kwargs={})
    job2 = Job(task_id="delayed.job", args=[], kwargs={})
    job3 = Job(task_id="failed.job", args=[], kwargs={})

    await backend.enqueue(queue, job1.to_dict())
    await backend.enqueue_delayed(queue, job2.to_dict(), run_at=9999999999)
    await backend.enqueue(queue, job3.to_dict())
    await backend.move_to_dlq(queue, job3.to_dict())

    waiting = await backend.list_jobs(queue, "waiting")
    delayed = await backend.list_jobs(queue, "delayed")
    failed = await backend.list_jobs(queue, "failed")

    assert all(j["task"] == "waiting.job" for j in waiting)
    assert all(j["task"] == "delayed.job" for j in delayed)
    assert all(j["task"] == "failed.job" for j in failed)


async def test_list_jobs_case_sensitive_state(backend):
    queue = "case-queue"
    job = Job(task_id="case.job", args=[], kwargs={})
    await backend.enqueue(queue, job.to_dict())

    jobs = await backend.list_jobs(queue, "Waiting")  # mixed case
    assert jobs == []  # or assert case-insensitive match if backend supports it


async def test_list_jobs_wrong_status_filter(backend):
    queue = "wrong-status-queue"
    job = Job(task_id="present.job", args=[], kwargs={})
    await backend.enqueue(queue, job.to_dict())

    # Ask for "failed", should return empty
    jobs = await backend.list_jobs(queue, "failed")
    assert jobs == []
