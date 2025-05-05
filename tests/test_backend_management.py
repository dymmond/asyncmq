import time

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend
from asyncmq.jobs import Job

pytestmark = pytest.mark.anyio


# Fixtures for each backend
@pytest.fixture
async def memory_backend():
    return InMemoryBackend()


@pytest.fixture
async def redis_backend():
    backend = RedisBackend(redis_url="redis://localhost:6379")
    # Flush DB before test
    await backend.redis.flushdb()
    yield backend
    # Cleanup
    await backend.redis.flushdb()


@pytest.fixture
async def postgres_backend():
    # Setup fresh schema
    await install_or_drop_postgres_backend()
    backend = PostgresBackend()
    await backend.connect()
    yield backend
    # Teardown schema and close
    await install_or_drop_postgres_backend(drop=True)
    await backend.close()


@pytest.fixture
async def mongodb_backend():
    backend = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database="test_asyncmq")
    # Ensure clean DB
    backend.store.client.drop_database("test_asyncmq")
    yield backend
    # Cleanup
    backend.store.client.drop_database("test_asyncmq")


async def _run_common_tests(backend):
    # 1) list_queues should start empty
    assert await backend.list_queues() == []

    # 2) enqueue into two queues with distinct job IDs
    job1 = Job(task_id="test.job", args=[], kwargs={})
    job2 = Job(task_id="test.job", args=[], kwargs={})
    await backend.enqueue("queueA", job1.to_dict())
    await backend.enqueue("queueB", job2.to_dict())

    queues = await backend.list_queues()
    assert set(queues) == {"queueA", "queueB"}

    # 3) register two workers on different queues
    ts1 = time.time()
    await backend.register_worker("worker1", "queueA", 5, ts1)
    ts2 = time.time()
    await backend.register_worker("worker2", "queueB", 3, ts2)

    workers = await backend.list_workers()
    ids = {w.id for w in workers}
    assert ids == {"worker1", "worker2"}

    # 4) deregister one and verify only the other remains
    await backend.deregister_worker("worker1")
    remaining = await backend.list_workers()
    remaining_ids = {w.id for w in remaining}
    assert remaining_ids == {"worker2"}


async def test_backend_queue_and_worker_management_in_memory(memory_backend):
    await _run_common_tests(memory_backend)


async def test_backend_queue_and_worker_management_redis(redis_backend):
    await _run_common_tests(redis_backend)


async def test_backend_queue_and_worker_management_postgres(postgres_backend):
    await _run_common_tests(postgres_backend)


async def test_backend_queue_and_worker_management_mongo(mongodb_backend):
    await _run_common_tests(mongodb_backend)
