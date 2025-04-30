import time

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend

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
    backend = RedisBackend(redis_url=REDIS_URL)
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
