# File: tests/test_sandbox_integration.py
import asyncio
import time
from contextlib import suppress
from uuid import uuid4

import pytest

import asyncmq.sandbox as sandbox_module
from asyncmq.backends.memory import InMemoryBackend
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.conf import monkay
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend
from asyncmq.jobs import Job
from asyncmq.runners import run_worker
from asyncmq.tasks import TASK_REGISTRY, task

pytestmark = pytest.mark.anyio


# Define tasks for sandbox tests
@task(queue="runner")
async def simple_task(x, y):
    return x + y


counter = {"calls": 0}


@task(queue="runner")
def flaky_task():
    counter["calls"] += 1
    # First call sleeps past timeout
    if counter["calls"] == 1:
        time.sleep(monkay.settings.sandbox_default_timeout + 0.1)
    return "recovered"


# Fixture to parametrize over backends
test_dbs = [
    "memory",
    "redis",
    "mongodb",
    "postgres",
]


@pytest.fixture(params=test_dbs)
async def backend(request, redis):
    name = request.param
    if name == "memory":
        yield InMemoryBackend()
    elif name == "redis":
        yield RedisBackend()
    elif name == "mongodb":
        db_name = f"test_asyncmq_{uuid4().hex}"
        b = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database=db_name)
        yield b
        await b.store.client.drop_database(db_name)
    elif name == "postgres":
        # Drop old and install schema
        await install_or_drop_postgres_backend(drop=True)
        await install_or_drop_postgres_backend(drop=False)
        b = PostgresBackend()
        yield b
        # Clean up
        await install_or_drop_postgres_backend(drop=True)
    else:
        pytest.skip(f"Unsupported backend: {name}")


async def run_one_job(queue_name, backend, job_id, **runner_kwargs):
    task_runner = asyncio.create_task(run_worker(queue_name, backend=backend, **runner_kwargs))
    try:
        start = time.time()
        while time.time() - start < 4.0:  # 🔼 increase timeout
            result = await backend.get_job_result(queue_name, job_id)
            if result is not None:
                return result
            await asyncio.sleep(0.02)
        raise TimeoutError(f"Job {job_id} did not complete in time")
    finally:
        task_runner.cancel()
        with suppress(asyncio.CancelledError):
            await task_runner


async def test_sandbox_execution_uses_run_handler(backend, monkeypatch):
    called = {"count": 0}
    sentinel = "sentinel"

    def fake_run_handler(task_id, args, kwargs, timeout):
        called["count"] += 1
        return sentinel

    monkeypatch.setattr(sandbox_module, "run_handler", fake_run_handler)

    monkay.settings.sandbox_enabled = True
    monkay.settings.sandbox_default_timeout = 0.1

    job_id = [k for k, v in TASK_REGISTRY.items() if v["func"] == simple_task][0]
    job = Job(task_id=job_id, args=[4, 5], kwargs={})
    await backend.enqueue("runner", job.to_dict())

    result = await run_one_job("runner", backend, job.id, concurrency=1)
    assert result == sentinel
    assert called["count"] == 1


async def test_flaky_task_retries_and_recovers_under_sandbox(backend):
    counter["calls"] = 0
    monkay.settings.sandbox_enabled = True
    monkay.settings.sandbox_default_timeout = 0.1

    job_id = [k for k, v in TASK_REGISTRY.items() if v["func"] == flaky_task][0]
    job = Job(task_id=job_id, args=[], kwargs={}, max_retries=1, backoff=0)
    await backend.enqueue("runner", job.to_dict())

    result = await run_one_job("runner", backend, job.id, concurrency=1)
    assert result == "recovered"
