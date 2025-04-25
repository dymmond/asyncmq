import fakeredis.aioredis as fake_redis_module  # fakeredis’s asyncio shim
import pytest
import redis.asyncio as real_redis_module  # the real redis.asyncio that your code imports

REDIS_URL = "redis://localhost:6379"
POSTGRES_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"

pytest_plugins = ("pytest_asyncio",)

@pytest.fixture(scope="session")
async def redis():
    """
    Provide a fakeredis.AsyncRedis instance whenever redis.asyncio.from_url is called.
    Flush on setup/teardown so tests are isolated.
    """
    # Create the fake-redis client
    client = fake_redis_module.FakeRedis(decode_responses=True)
    await client.flushall()

    # Monkey-patch the real.from_url to return our fake client
    # so that RedisBackend() will pick it up automatically.
    real_redis_module.from_url = lambda *args, **kwargs: client

    yield client

    # Cleanup
    await client.flushall()
    await client.close()
