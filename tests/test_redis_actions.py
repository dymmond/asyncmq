import json
import time

import pytest

from asyncmq.backends.redis import RedisBackend

pytestmark = pytest.mark.asyncio


async def test_cancel_job(redis):
    backend = RedisBackend(redis_url_or_client="redis://localhost:6379")
    backend.redis = redis

    job = {"id": "r1"}
    await backend.enqueue("q1", job)

    await backend.cancel_job("q1", "r1")

    waiting = await redis.zrange("queue:q1:waiting", 0, -1)
    assert not any(json.loads(m)["id"] == "r1" for m in waiting)

    cancelled = await redis.smembers("queue:q1:cancelled")
    assert "r1" in cancelled


async def test_retry_job(redis):
    backend = RedisBackend(redis_url_or_client="redis://localhost:6379")
    backend.redis = redis

    job = {"id": "r2"}
    await backend.enqueue("q1", job)
    await backend.move_to_dlq("q1", job)

    result = await backend.retry_job("q1", "r2")
    assert result is True

    waiting = await redis.zrange("queue:q1:waiting", 0, -1)
    assert any(json.loads(m)["id"] == "r2" for m in waiting)


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

    assert not any(json.loads(m)["id"] == "r3" for m in waiting)
    assert not any(json.loads(m)["id"] == "r3" for m in delayed)
    assert not any(json.loads(m)["id"] == "r3" for m in dlq)
