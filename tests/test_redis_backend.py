# tests/test_redis_backend.py
import asyncio
import time

import pytest

from asyncqueue.backends.redis import RedisBackend
from asyncqueue.job import Job


@pytest.mark.asyncio
async def test_enqueue_and_dequeue(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.enqueue", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    result = await backend.dequeue("test")
    assert result["id"] == job.id


@pytest.mark.asyncio
async def test_job_state_tracking(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.state", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    await backend.update_job_state("test", job.id, "active")
    state = await backend.get_job_state("test", job.id)
    assert state == "active"


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
    await asyncio.sleep(0.4)
    due = await backend.get_due_delayed("test")
    assert any(j["id"] == job.id for j in due)


@pytest.mark.asyncio
async def test_move_to_dlq(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.dlq", args=[], kwargs={})
    await backend.move_to_dlq("test", job.to_dict())
    state = await backend.get_job_state("test", job.id)
    assert state == "failed"


@pytest.mark.asyncio
async def test_remove_delayed(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.remove_delayed", args=[], kwargs={})
    run_at = time.time() + 1.0
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    await backend.remove_delayed("test", job.id)
    delayed = await backend.get_due_delayed("test")
    assert all(j["id"] != job.id for j in delayed)
