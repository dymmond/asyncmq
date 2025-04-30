import pytest
import pytest_asyncio
import redis.asyncio as async_redis

REDIS_URL = "redis://localhost:6379"

# Use pytest-asyncio
pytest_plugins = ("pytest_asyncio",)


@pytest_asyncio.fixture(scope="function")
async def redis():
    """
    Provide a real Redis client for testing.
    Make sure Redis is running locally on 6379.
    """
    client = async_redis.from_url(REDIS_URL, decode_responses=True)
    # start from a clean slate
    await client.flushall()
    yield client
    # teardown
    await client.flushall()
    await client.close()


@pytest.fixture(scope="module", params=["asyncio", "trio"])
def anyio_backend():
    return ("asyncio", {"debug": True})
