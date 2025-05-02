import asyncio
import json
import time

import pytest

from asyncmq.backends.redis import RedisBackend
from asyncmq.core.enums import State
from asyncmq.jobs import Job
from asyncmq.logging import logger


@pytest.mark.asyncio
async def test_enqueue_and_dequeue(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.enqueue", args=[], kwargs={})
    job_payload = job.to_dict()  # Capture payload to compare later

    await backend.enqueue("test", job_payload)
    waiting_key = backend._waiting_key("test")
    zmembers = await redis.zrange(waiting_key, 0, -1)

    if zmembers:
        try:
            decoded = [json.loads(item) for item in zmembers]
            logger.info(f"Redis waiting‐set content (decoded): {decoded}")
        except json.JSONDecodeError:
            logger.info("Could not decode sorted‐set content as JSON.")

    result = await backend.dequeue("test")
    assert result["id"] == job.id


@pytest.mark.asyncio
async def test_job_state_tracking(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.state", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    await backend.update_job_state("test", job.id, State.ACTIVE)
    state = await backend.get_job_state("test", job.id)
    assert state == State.ACTIVE


@pytest.mark.asyncio
async def test_job_result_handling(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.result", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    await backend.save_job_result("test", job.id, 9876)
    result = await backend.get_job_result("test", job.id)
    assert result == 9876


@pytest.mark.asyncio
async def test_enqueue_delayed_and_due(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.delayed", args=[], kwargs={})
    run_at = time.time() + 0.3
    await backend.enqueue_delayed("test", job.to_dict(), run_at)

    await asyncio.sleep(0.6)  # Increased delay
    due = await backend.get_due_delayed("test")
    print("DUE JOBS:", due)

    assert any(j.get("id") == job.id for j in due)


@pytest.mark.asyncio
async def test_move_to_dlq(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.dlq", args=[], kwargs={})
    await backend.move_to_dlq("test", job.to_dict())
    state = await backend.get_job_state("test", job.id)
    assert state == State.FAILED


@pytest.mark.asyncio
async def test_remove_delayed(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.remove_delayed", args=[], kwargs={})
    run_at = time.time() + 1.0
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    await backend.remove_delayed("test", job.id)
    delayed = await backend.get_due_delayed("test")
    assert all(j["id"] != job.id for j in delayed)


@pytest.mark.parametrize("state", ["waiting", "delayed", "failed"])
@pytest.mark.asyncio
async def test_list_jobs_by_state(state):
    backend = RedisBackend()
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
@pytest.mark.asyncio
async def test_list_jobs_empty_queue(state):
    backend = RedisBackend()
    jobs = await backend.list_jobs("empty-queue", state)
    assert isinstance(jobs, list)
    assert len(jobs) == 0


@pytest.mark.asyncio
async def test_list_jobs_filters_correctly():
    backend = RedisBackend()
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
