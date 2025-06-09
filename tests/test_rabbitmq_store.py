import pytest
import pytest_asyncio

from asyncmq.stores.rabbitmq import RabbitMQJobStore

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(scope="function")
async def redis_store(redis):
    # Underlying Redis store
    store = RabbitMQJobStore(redis_url=None, backend=redis)
    # override to use redis fixture
    # Actually, RabbitMQJobStore delegates to RedisJobStore
    store._store.redis = redis
    await redis.flushall()
    return store


async def test_save_and_load(redis_store):
    await redis_store.save("q1", "j1", {"a": 1})
    data = await redis_store.load("q1", "j1")
    assert data == {"a": 1}


async def test_delete(redis_store):
    await redis_store.save("q1", "j2", {"b": 2})
    await redis_store.delete("q1", "j2")
    assert await redis_store.load("q1", "j2") is None


async def test_all_jobs_and_jobs_by_status(redis_store):
    # Save multiple jobs with different statuses
    await redis_store.save("q2", "j3", {"status": "waiting"})
    await redis_store.save("q2", "j4", {"status": "active"})
    all_jobs = await redis_store.all_jobs("q2")
    assert any(j.get("status") == "waiting" for j in all_jobs)
    assert any(j.get("status") == "active" for j in all_jobs)

    waiting = await redis_store.jobs_by_status("q2", "waiting")
    assert all(j.get("status") == "waiting" for j in waiting)
