import pytest
import fakeredis
import redis.asyncio as real_redis_module

REDIS_URL = "redis://localhost:6379"
POSTGRES_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"

pytest_plugins = ("pytest_asyncio",)

@pytest.fixture(scope="function")
async def redis(monkeypatch):
    """
    Provide a fake Redis client for testing, and patch redis.asyncio.from_url to return it.
    """
    # Create the fake-redis client
    client = fakeredis.FakeRedis(decode_responses=True)
    # Monkey-patch the real from_url so that RedisBackend picks up the fake
    monkeypatch.setattr(real_redis_module, 'from_url', lambda *args, **kwargs: client)
    # Ensure a clean state before the test
    await client.flushall()
    yield client
    # Teardown: flush and close
    await client.flushall()
    await client.close()
