from uuid import uuid4

import anyio
import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.enums import State
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend
from asyncmq.queues import Queue
from asyncmq.tasks import TASK_REGISTRY, task

pytestmark = pytest.mark.anyio


@task(queue="dedup")
async def dedup_task(value=None):
    return value


def get_task_id() -> str:
    for task_id, meta in TASK_REGISTRY.items():
        if meta["func"] == dedup_task:
            return task_id
    raise AssertionError("dedup_task not registered")


@pytest.fixture(params=["memory", "redis", "mongodb", "postgres"])
async def backend(request):
    name = request.param
    if name == "memory":
        yield InMemoryBackend()
        return

    if name == "redis":
        backend = RedisBackend()
        await backend.redis.flushall()
        try:
            yield backend
        finally:
            await backend.redis.flushall()
            await backend.redis.aclose()
            await backend.job_store.redis.aclose()
        return

    if name == "mongodb":
        db_name = f"test_asyncmq_{uuid4().hex}"
        backend = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database=db_name)
        yield backend
        await backend.store.client.drop_database(db_name)
        return

    if name == "postgres":
        await install_or_drop_postgres_backend(drop=True)
        await install_or_drop_postgres_backend(drop=False)
        backend = PostgresBackend()
        yield backend
        await install_or_drop_postgres_backend(drop=True)
        return

    pytest.skip(f"Unsupported backend: {name}")


async def test_simple_deduplication_ignores_duplicate_jobs(backend):
    queue = Queue(f"dedup-simple-{uuid4().hex}", backend=backend)
    task_id = get_task_id()

    first = await queue.add(task_id, args=["first"], deduplication={"id": "simple-key"})
    second = await queue.add(task_id, args=["second"], deduplication={"id": "simple-key"})

    waiting = await queue.get_waiting(asc=True)

    assert second == first
    assert await queue.get_deduplication_job_id("simple-key") == first
    assert [job["id"] for job in waiting] == [first]
    assert waiting[0]["args"] == ["first"]


async def test_remove_deduplication_key_allows_new_job(backend):
    queue = Queue(f"dedup-remove-{uuid4().hex}", backend=backend)
    task_id = get_task_id()

    first = await queue.add(task_id, args=["first"], deduplication={"id": "release-key"})

    assert await queue.remove_deduplication_key("release-key") is True

    second = await queue.add(task_id, args=["second"], deduplication={"id": "release-key"})
    waiting = await queue.get_waiting(asc=True)

    assert first != second
    assert await queue.get_deduplication_job_id("release-key") == second
    assert [job["args"][0] for job in waiting] == ["first", "second"]


async def test_throttle_deduplication_persists_until_ttl_expires(backend):
    queue = Queue(f"dedup-throttle-{uuid4().hex}", backend=backend)
    task_id = get_task_id()

    first = await queue.add(task_id, args=["first"], deduplication={"id": "throttle-key", "ttl": 0.2})
    await queue.backend.update_job_state(queue.name, first, State.COMPLETED)

    duplicate = await queue.add(task_id, args=["duplicate"], deduplication={"id": "throttle-key", "ttl": 0.2})
    assert duplicate == first

    await anyio.sleep(0.25)

    third = await queue.add(task_id, args=["third"], deduplication={"id": "throttle-key", "ttl": 0.2})
    assert third != first


async def test_debounce_replaces_existing_delayed_job(backend):
    queue = Queue(f"dedup-debounce-{uuid4().hex}", backend=backend)
    task_id = get_task_id()

    first = await queue.add(
        task_id,
        args=["first"],
        delay=1.0,
        deduplication={"id": "debounce-key", "ttl": 1.0, "extend": True, "replace": True},
    )
    second = await queue.add(
        task_id,
        args=["second"],
        delay=1.0,
        deduplication={"id": "debounce-key", "ttl": 1.0, "extend": True, "replace": True},
    )

    delayed = await queue.get_delayed(asc=True)

    assert second != first
    assert await queue.get_deduplication_job_id("debounce-key") == second
    assert [job["id"] for job in delayed] == [second]
    assert delayed[0]["args"] == ["second"]
