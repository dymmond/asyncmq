import time

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend
from asyncmq.queues import Queue

REDIS_URL = "redis://localhost:6379"
MONGO_URL = "mongodb://root:mongoadmin@localhost:27017"
MONGO_DB = "test_asyncmq"


@pytest.mark.asyncio
async def test_queue_stats_inmemory():
    backend = InMemoryBackend()
    # waiting jobs
    job1 = {"id": "j1"}
    job2 = {"id": "j2"}
    await backend.enqueue("test-queue", job1)
    await backend.enqueue("test-queue", job2)
    # delayed jobs
    run_at = time.time() + 60
    job3 = {"id": "j3"}
    await backend.enqueue_delayed("test-queue", job3, run_at)
    # failed jobs
    job4 = {"id": "j4"}
    await backend.enqueue("test-queue", job4)
    await backend.move_to_dlq("test-queue", job4)
    stats = await backend.queue_stats("test-queue")
    assert stats == {"waiting": 2, "delayed": 1, "failed": 1}


@pytest.mark.asyncio
async def test_queue_stats_redis(redis):
    backend = RedisBackend(redis_url_or_client=REDIS_URL)
    # waiting jobs
    job1 = {"id": "rj1"}
    job2 = {"id": "rj2"}
    await backend.enqueue("test-queue", job1)
    await backend.enqueue("test-queue", job2)
    # delayed jobs
    run_at = time.time() + 60
    job3 = {"id": "rj3"}
    await backend.enqueue_delayed("test-queue", job3, run_at)
    # failed jobs
    job4 = {"id": "rj4"}
    await backend.enqueue("test-queue", job4)
    await backend.move_to_dlq("test-queue", job4)
    stats = await backend.queue_stats("test-queue")
    assert stats == {"waiting": 2, "delayed": 1, "failed": 1}


@pytest.mark.asyncio
async def test_queue_stats_postgres():
    # Set up the database schema
    await install_or_drop_postgres_backend()
    backend = PostgresBackend()
    await backend.connect()
    try:
        # waiting jobs
        job1 = {"id": "pj1", "task_id": "t", "args": [], "kwargs": {}}
        job2 = {"id": "pj2", "task_id": "t", "args": [], "kwargs": {}}
        await backend.enqueue("test-queue", job1)
        await backend.enqueue("test-queue", job2)
        # delayed jobs
        run_at = time.time() + 60
        job3 = {"id": "pj3", "task_id": "t", "args": [], "kwargs": {}}
        await backend.enqueue_delayed("test-queue", job3, run_at)
        # failed jobs
        job4 = {"id": "pj4", "task_id": "t", "args": [], "kwargs": {}}
        await backend.enqueue("test-queue", job4)
        await backend.move_to_dlq("test-queue", job4)
        stats = await backend.queue_stats("test-queue")
        assert stats == {"waiting": 2, "delayed": 1, "failed": 1}
    finally:
        await backend.close()
        await install_or_drop_postgres_backend(drop=True)


@pytest.mark.asyncio
async def test_queue_stats_mongodb():
    # Initialize and clean the database
    backend = MongoDBBackend(mongo_url=MONGO_URL, database=MONGO_DB)
    backend.store.client.drop_database(MONGO_DB)
    # waiting jobs
    job1 = {"id": "mj1", "task_id": "t", "args": [], "kwargs": {}}
    job2 = {"id": "mj2", "task_id": "t", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job1)
    await backend.enqueue("test-queue", job2)
    # delayed jobs
    run_at = time.time() + 60
    job3 = {"id": "mj3", "task_id": "t", "args": [], "kwargs": {}}
    await backend.enqueue_delayed("test-queue", job3, run_at)
    # failed jobs
    job4 = {"id": "mj4", "task_id": "t", "args": [], "kwargs": {}}
    await backend.enqueue("test-queue", job4)
    await backend.move_to_dlq("test-queue", job4)
    stats = await backend.queue_stats("test-queue")
    assert stats == {"waiting": 2, "delayed": 1, "failed": 1}
    # Cleanup
    backend.store.client.drop_database(MONGO_DB)


@pytest.mark.asyncio
async def test_queue_enqueue_returns_id_inmemory():
    backend = InMemoryBackend()
    q = Queue("qa-test", backend=backend)

    # No ID provided -> should mint one and return it, and increase waiting count
    jid = await q.enqueue({"task_id": "t", "args": [], "kwargs": {}})
    assert isinstance(jid, str) and len(jid) > 0

    stats = await backend.queue_stats("qa-test")
    assert stats == {"waiting": 1, "delayed": 0, "failed": 0}

    # Custom ID provided -> must be preserved
    jid2 = await q.enqueue({"id": "custom-id-1", "task_id": "t", "args": [], "kwargs": {}})
    assert jid2 == "custom-id-1"

    stats = await backend.queue_stats("qa-test")
    assert stats == {"waiting": 2, "delayed": 0, "failed": 0}


@pytest.mark.asyncio
async def test_queue_enqueue_delayed_and_delay_wrapper_inmemory():
    backend = InMemoryBackend()
    q = Queue("qa-delayed", backend=backend)

    # Direct delayed
    run_at = time.time() + 60
    djid = await q.enqueue_delayed({"task_id": "t", "args": [], "kwargs": {}}, run_at)
    assert isinstance(djid, str) and len(djid) > 0

    stats = await backend.queue_stats("qa-delayed")
    assert stats == {"waiting": 0, "delayed": 1, "failed": 0}

    # delay() immediate path
    jid = await q.delay({"task_id": "t", "args": [], "kwargs": {}}, run_at=None)
    assert isinstance(jid, str)

    # delay() delayed path
    run_at2 = time.time() + 120
    djid2 = await q.delay({"task_id": "t", "args": [], "kwargs": {}}, run_at=run_at2)
    assert isinstance(djid2, str)

    stats = await backend.queue_stats("qa-delayed")
    # waiting +1, delayed +1 more (total delayed 2)
    assert stats == {"waiting": 1, "delayed": 2, "failed": 0}


@pytest.mark.asyncio
async def test_queue_add_and_add_bulk_inmemory():
    backend = InMemoryBackend()
    q = Queue("qa-add", backend=backend)

    jid = await q.add("t", args=[1], kwargs={"a": 2}, retries=2, ttl=60, priority=3)
    assert isinstance(jid, str) and len(jid) > 0

    jobs_cfg = [
        {"task_id": "t", "args": [1], "kwargs": {"k": "v1"}},
        {"task_id": "t", "args": [], "kwargs": {}},
        {"task_id": "t", "args": [7, 8], "kwargs": {"x": 9}, "retries": 1, "priority": 1},
    ]
    ids = await q.add_bulk(jobs_cfg)
    assert len(ids) == 3
    assert all(isinstance(x, str) and len(x) > 0 for x in ids)

    stats = await backend.queue_stats("qa-add")
    # 1 (from add) + 3 (from add_bulk)
    assert stats == {"waiting": 4, "delayed": 0, "failed": 0}


@pytest.mark.asyncio
async def test_queue_enqueue_and_delayed_redis(redis):
    backend = RedisBackend(redis_url_or_client=REDIS_URL)
    q = Queue("qa-redis", backend=backend)

    jid = await q.enqueue({"task_id": "t", "args": [], "kwargs": {}})
    assert isinstance(jid, str) and len(jid) > 0

    run_at = time.time() + 45
    djid = await q.enqueue_delayed({"task_id": "t", "args": [], "kwargs": {}}, run_at)
    assert isinstance(djid, str)

    stats = await backend.queue_stats("qa-redis")
    assert stats == {"waiting": 1, "delayed": 1, "failed": 0}


@pytest.mark.asyncio
async def test_queue_enqueue_and_delayed_postgres():
    await install_or_drop_postgres_backend()
    backend = PostgresBackend()
    await backend.connect()
    try:
        q = Queue("qa-postgres", backend=backend)

        jid = await q.enqueue({"task_id": "t", "args": [], "kwargs": {}})
        assert isinstance(jid, str) and len(jid) > 0

        run_at = time.time() + 30
        djid = await q.enqueue_delayed({"task_id": "t", "args": [], "kwargs": {}}, run_at)
        assert isinstance(djid, str)

        stats = await backend.queue_stats("qa-postgres")
        assert stats == {"waiting": 1, "delayed": 1, "failed": 0}

        # add_bulk also returns ids and increases waiting
        ids = await q.add_bulk(
            [
                {"task_id": "t", "args": [], "kwargs": {}},
                {"task_id": "t", "args": [1], "kwargs": {"a": 2}},
            ]
        )
        assert len(ids) == 2
        stats = await backend.queue_stats("qa-postgres")
        assert stats == {"waiting": 3, "delayed": 1, "failed": 0}
    finally:
        await backend.close()
        await install_or_drop_postgres_backend(drop=True)


@pytest.mark.asyncio
async def test_queue_enqueue_and_delayed_mongodb():
    backend = MongoDBBackend(mongo_url=MONGO_URL, database=MONGO_DB)
    backend.store.client.drop_database(MONGO_DB)
    try:
        q = Queue("qa-mongo", backend=backend)

        jid = await q.enqueue({"task_id": "t", "args": [], "kwargs": {}})
        assert isinstance(jid, str) and len(jid) > 0

        run_at = time.time() + 75
        djid = await q.enqueue_delayed({"task_id": "t", "args": [], "kwargs": {}}, run_at)
        assert isinstance(djid, str)

        stats = await backend.queue_stats("qa-mongo")
        assert stats == {"waiting": 1, "delayed": 1, "failed": 0}

        # delay() immediate + delayed paths
        _ = await q.delay({"task_id": "t", "args": [], "kwargs": {}}, run_at=None)
        _ = await q.delay({"task_id": "t", "args": [], "kwargs": {}}, run_at=time.time() + 5)

        stats = await backend.queue_stats("qa-mongo")
        assert stats == {"waiting": 2, "delayed": 2, "failed": 0}
    finally:
        backend.store.client.drop_database(MONGO_DB)
