import pytest
import redis.asyncio as redis

REDIS_URL = "redis://localhost:6379"
POSTGRES_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"

pytest_plugins = ("pytest_asyncio",)

@pytest.fixture(scope="session")
async def redis():
    client = redis.from_url(REDIS_URL, decode_responses=True)
    yield client
    await client.flushall()
    await client.close()
