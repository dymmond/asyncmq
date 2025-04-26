# conftest.py
# Use the async specific fake client
# Make sure you have installed fakeredis[asyncio]
import pytest_asyncio
import redis.asyncio as real_redis_module
from fakeredis.aioredis import FakeRedis

REDIS_URL = "redis://localhost:6379"
POSTGRES_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"

# You likely still need this plugin import if not using auto mode globally
pytest_plugins = ("pytest_asyncio",)

@pytest_asyncio.fixture(scope="function") # Keep the correct decorator
async def redis(monkeypatch):
    """
    Provide a fake Redis client for testing, and patch redis.asyncio.from_url to return it.
    """
    # Create the async fake-redis client
    # Use the async specific FakeRedis class from fakeredis.asyncio
    client = FakeRedis(decode_responses=True)

    # Monkey-patch the real from_url so that RedisBackend picks up the fake
    # This patch correctly makes backend/store use the 'client' instance
    monkeypatch.setattr(real_redis_module, 'from_url', lambda *args, **kwargs: client)

    # Ensure a clean state before the test
    # These methods should now be awaitable on the async fake client
    await client.flushall()

    yield client # This yields the async fakeredis client

    # Teardown: flush and close
    await client.flushall()
    # Close should also be awaitable on the async fake client
    await client.close()
