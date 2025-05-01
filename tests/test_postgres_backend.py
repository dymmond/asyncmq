import asyncio
import time

import pytest

from asyncmq.backends.postgres import PostgresBackend
from asyncmq.core.enums import State
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend
from asyncmq.jobs import Job

pytestmark = pytest.mark.anyio


@pytest.fixture(scope="module")
async def backend():
    await install_or_drop_postgres_backend()
    backend = PostgresBackend()
    await backend.connect()
    yield backend
    await install_or_drop_postgres_backend(drop=True)
    await backend.close()


async def test_enqueue_and_dequeue(backend):
    job = {"id": "job1", "task_id": "test_task", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job)
    dequeued = await backend.dequeue("test-queue")
    assert dequeued["id"] == "job1"


async def test_ack(backend):
    job = {"id": "job2", "task_id": "test_task", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job)
    await backend.ack("test-queue", job_id="job2")
    result = await backend.get_job_state("test-queue", job_id="job2")
    assert result is None


async def test_move_to_dlq(backend):
    job = {"id": "job3", "task_id": "test_task", "args": [], "kwargs": {}}
    await backend.move_to_dlq("test-queue", job)
    state = await backend.get_job_state("test-queue", job_id="job3")
    assert state == State.FAILED


async def test_delayed_job_flow(backend):
    job = {"id": "job4", "task_id": "delayed_task", "args": [], "kwargs": {}}
    run_at = time.time() + 1
    await backend.enqueue_delayed("test-queue", job, run_at)

    # Immediately: no jobs ready
    due_jobs = await backend.get_due_delayed("test-queue")
    assert due_jobs == []

    # Wait for delay
    await asyncio.sleep(1.5)
    due_jobs = await backend.get_due_delayed("test-queue")
    assert any(j["id"] == "job4" for j in due_jobs)

    # Remove from delayed
    await backend.remove_delayed("test-queue", "job4")


async def test_save_and_get_job_result(backend):
    job = {"id": "job5", "task_id": "task_result", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job)
    await backend.save_job_result("test-queue", "job5", {"value": 42})
    result = await backend.get_job_result("test-queue", "job5")
    assert result == {"value": 42}


async def test_dependencies(backend):
    job = {"id": "job6", "task_id": "task_dep", "args": [], "kwargs": {}, "depends_on": ["parent1"]}
    await backend.enqueue("test-queue", job)
    await backend.add_dependencies("test-queue", job)
    await backend.resolve_dependency("test-queue", "parent1")
    state = await backend.get_job_state("test-queue", "job6")
    assert state == State.WAITING


async def test_save_job_progress(backend):
    job = {"id": "job7", "task_id": "progress_task", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job)
    await backend.save_job_progress("test-queue", "job7", 0.5)
    loaded = await backend.store.load("test-queue", "job7")
    assert loaded["progress"] == 0.5


async def test_bulk_enqueue(backend):
    jobs = [{"id": f"bulk-{i}", "task_id": "bulk_task", "args": [], "kwargs": {}} for i in range(5)]
    await backend.bulk_enqueue("test-queue", jobs)
    all_jobs = await backend.store.all_jobs("test-queue")
    assert len([job for job in all_jobs if job.get("task_id") == "bulk_task" or job.get("task") == "bulk_task"]) >= 5


async def test_purge(backend):
    job = {"id": "oldjob", "task_id": "old_task", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job)
    await backend.update_job_state("test-queue", "oldjob", State.COMPLETED)
    await asyncio.sleep(1)
    await backend.purge("test-queue", state=State.COMPLETED, older_than=0.5)
    state = await backend.get_job_state("test-queue", "oldjob")
    assert state is None


async def test_distributed_lock(backend):
    lock = await backend.create_lock("test-lock", ttl=10)
    acquired = await lock.acquire()
    assert acquired
    await lock.release()


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
