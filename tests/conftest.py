import importlib
import pkgutil

import pytest
import pytest_asyncio
import redis.asyncio as async_redis

import tests
from asyncmq.tasks import TASK_REGISTRY

REDIS_URL = "redis://localhost:6379"

# Use pytest-asyncio
pytest_plugins = ("pytest_asyncio",)


@pytest.fixture(autouse=True)
def reset_task_registry():
    # Clear any leftover registrations
    TASK_REGISTRY.clear()

    # Re-import every test module under tests/ so their @task decorators run
    for _, name, _ in pkgutil.walk_packages(tests.__path__, tests.__name__ + "."):
        if "tests.dashboard" in name:
            # If we import the dashboard tests,
            # ALL tests will be skipped due to the `importorskip` call.
            continue
        importlib.reload(importlib.import_module(name))

    yield

    # (Optional) clear again after each test
    TASK_REGISTRY.clear()


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
    await client.aclose()


@pytest.fixture(scope="module", params=["asyncio", "trio"])
def anyio_backend():
    return ("asyncio", {"debug": True})
