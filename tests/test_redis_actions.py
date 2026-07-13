import json
import time

import pytest

from asyncmq.backends.redis import RedisBackend

pytestmark = pytest.mark.asyncio


def _member_job_id(member):
    raw = member.decode("utf-8") if isinstance(member, bytes) else member
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    return payload.get("id") if isinstance(payload, dict) else raw


async def test_cancel_job(redis):
    backend = RedisBackend(redis_url_or_client="redis://localhost:6379")
    backend.redis = redis

    job = {"id": "r1"}
    await backend.enqueue("q1", job)

    await backend.cancel_job("q1", "r1")

    waiting = await redis.zrange("queue:q1:waiting", 0, -1)
    assert not any(_member_job_id(m) == "r1" for m in waiting)

    cancelled = await redis.smembers("queue:q1:cancelled")
    assert "r1" in cancelled


async def test_retry_job(redis):
    backend = RedisBackend(redis_url_or_client=redis)

    job = {"id": "r2", "status": "failed", "result": "old", "last_error": "old failure"}
    await backend.enqueue("q1", job)
    await backend.move_to_dlq("q1", job)

    result = await backend.retry_job("q1", "r2")
    assert result is True

    waiting = await redis.zrange("queue:q1:waiting", 0, -1)
    assert any(_member_job_id(m) == "r2" for m in waiting)
    retried = await backend.get_job("q1", "r2")
    assert retried is not None
    assert retried["status"] == "waiting"
    assert "result" not in retried
    assert "last_error" not in retried


async def test_remove_job(redis):
    backend = RedisBackend(redis_url_or_client="redis://localhost:6379")
    backend.redis = redis

    job = {"id": "r3"}
    await backend.enqueue("q1", job)
    await backend.enqueue_delayed("q1", job, time.time())
    await backend.move_to_dlq("q1", job)

    result = await backend.remove_job("q1", "r3")
    assert result is True

    waiting = await redis.zrange("queue:q1:waiting", 0, -1)
    delayed = await redis.zrange("queue:q1:delayed", 0, -1)
    dlq = await redis.zrange("queue:q1:dlq", 0, -1)

    assert not any(_member_job_id(m) == "r3" for m in waiting)
    assert not any(json.loads(m)["id"] == "r3" for m in delayed)
    assert not any(json.loads(m)["id"] == "r3" for m in dlq)
    assert await backend.get_job_state("q1", "r3") is None


async def test_remove_job_clears_redis_cancellation_marker(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue, job_id = "q1", "r-cancelled-remove"

    await backend.cancel_job(queue, job_id)
    assert await backend.is_job_cancelled(queue, job_id)

    assert await backend.remove_job(queue, job_id) is True

    assert not await backend.is_job_cancelled(queue, job_id)
