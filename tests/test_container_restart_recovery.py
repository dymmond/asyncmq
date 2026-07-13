import subprocess
import time
from collections.abc import Awaitable, Callable
from typing import Any

import anyio
import pytest

from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.rabbitmq import RabbitMQBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.enums import State
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend

pytestmark = pytest.mark.anyio

REDIS_URL = "redis://localhost:6379"
POSTGRES_DSN = "postgresql://postgres:postgres@localhost:5432/postgres"
MONGO_URL = "mongodb://root:mongoadmin@localhost:27017"
RABBIT_URL = "amqp://guest:guest@localhost/"


def _require_container(container: str) -> None:
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"Docker container {container!r} is not available")


async def _restart_container(container: str, wait: Callable[[], Awaitable[None]]) -> None:
    _require_container(container)
    result = await anyio.to_thread.run_sync(
        lambda: subprocess.run(["docker", "restart", container], check=False, capture_output=True, text=True)
    )
    if result.returncode != 0:
        pytest.fail(f"docker restart {container} failed\nstdout={result.stdout}\nstderr={result.stderr}")

    with anyio.fail_after(20):
        while True:
            try:
                await wait()
                return
            except Exception:
                await anyio.sleep(0.2)


async def _wait_redis() -> None:
    backend = RedisBackend(REDIS_URL)
    try:
        await backend.health_check()
    finally:
        await backend.redis.aclose()
        await backend.job_store.redis.aclose()


async def _wait_postgres() -> None:
    backend = PostgresBackend(dsn=POSTGRES_DSN)
    try:
        await backend.health_check()
    finally:
        await backend.close()


async def _wait_mongo() -> None:
    backend = MongoDBBackend(mongo_url=MONGO_URL, database="asyncmq_restart_wait")
    try:
        await backend.health_check()
    finally:
        backend.store.client.close()


async def _wait_rabbitmq() -> None:
    backend = RabbitMQBackend(rabbit_url=RABBIT_URL, redis_url=REDIS_URL, max_priority=None)
    try:
        await backend.health_check()
    finally:
        await backend.close()


def _active_payload(job_id: str) -> dict[str, Any]:
    return {"id": job_id, "task": "restart.recover", "args": [], "kwargs": {}, "priority": 1}


async def test_redis_stalled_recovery_survives_container_restart():
    queue = "container-restart-redis"
    job_id = "container-restart-redis-job"
    producer = RedisBackend(REDIS_URL)
    await producer.redis.flushdb()

    try:
        await producer.enqueue(queue, _active_payload(job_id))
        raw = await producer.dequeue(queue)
        assert raw is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)
        await producer.redis.save()

        await producer.redis.aclose()
        await producer.job_store.redis.aclose()
        await _restart_container("taskq_redis", _wait_redis)

        recovery = RedisBackend(REDIS_URL)
        try:
            stalled = await recovery.fetch_stalled_jobs(old + 1)
            entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
            await recovery.reenqueue_stalled(queue, entry["job_data"])

            assert await recovery.get_job_state(queue, job_id) == State.WAITING
            recovered = await recovery.dequeue(queue)
            assert recovered is not None
            assert recovered["id"] == job_id
        finally:
            await recovery.redis.flushdb()
            await recovery.redis.aclose()
            await recovery.job_store.redis.aclose()
    finally:
        await _wait_redis()
        cleanup = RedisBackend(REDIS_URL)
        await cleanup.redis.flushdb()
        await cleanup.redis.aclose()
        await cleanup.job_store.redis.aclose()


async def test_postgres_stalled_recovery_survives_container_restart():
    queue = "container-restart-postgres"
    job_id = "container-restart-postgres-job"

    await install_or_drop_postgres_backend(drop=True)
    await install_or_drop_postgres_backend()
    producer = PostgresBackend(dsn=POSTGRES_DSN)

    try:
        await producer.connect()
        await producer.enqueue(queue, _active_payload(job_id))
        raw = await producer.dequeue(queue)
        assert raw is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)
        await producer.close()

        await _restart_container("taskq_postgres", _wait_postgres)

        recovery = PostgresBackend(dsn=POSTGRES_DSN)
        try:
            await recovery.connect()
            stalled = await recovery.fetch_stalled_jobs(old + 1)
            entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
            await recovery.reenqueue_stalled(queue, entry["job_data"])

            assert await recovery.get_job_state(queue, job_id) == State.WAITING
            recovered = await recovery.dequeue(queue)
            assert recovered is not None
            assert recovered["id"] == job_id
        finally:
            await recovery.close()
    finally:
        await _wait_postgres()
        await install_or_drop_postgres_backend(drop=True)


async def test_mongodb_stalled_recovery_survives_container_restart():
    queue = "container-restart-mongo"
    job_id = "container-restart-mongo-job"
    database = "asyncmq_container_restart"
    producer = MongoDBBackend(mongo_url=MONGO_URL, database=database)
    producer.store.client.drop_database(database)

    try:
        await producer.connect()
        await producer.enqueue(queue, _active_payload(job_id))
        raw = await producer.dequeue(queue)
        assert raw is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)
        producer.store.client.close()

        await _restart_container("mongo", _wait_mongo)

        recovery = MongoDBBackend(mongo_url=MONGO_URL, database=database)
        try:
            await recovery.connect()
            stalled = await recovery.fetch_stalled_jobs(old + 1)
            entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
            await recovery.reenqueue_stalled(queue, entry["job_data"])

            assert await recovery.get_job_state(queue, job_id) == State.WAITING
            recovered = await recovery.dequeue(queue)
            assert recovered is not None
            assert recovered["id"] == job_id
        finally:
            recovery.store.client.drop_database(database)
            recovery.store.client.close()
    finally:
        await _wait_mongo()
        cleanup = MongoDBBackend(mongo_url=MONGO_URL, database=database)
        cleanup.store.client.drop_database(database)
        cleanup.store.client.close()


async def test_rabbitmq_stalled_recovery_survives_container_restart():
    queue = "container_restart_rabbit"
    job_id = "container-restart-rabbit-job"
    producer = RabbitMQBackend(rabbit_url=RABBIT_URL, redis_url=REDIS_URL, max_priority=None)
    metadata = RedisBackend(REDIS_URL)
    await metadata.redis.flushdb()

    try:
        await producer.drain_queue(queue)
        await producer.enqueue(queue, _active_payload(job_id))
        raw = await producer.dequeue(queue)
        assert raw is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)
        await producer.close()

        await _restart_container("asyncmq-rabbitmq-1", _wait_rabbitmq)

        recovery = RabbitMQBackend(rabbit_url=RABBIT_URL, redis_url=REDIS_URL, max_priority=None)
        try:
            stalled = await recovery.fetch_stalled_jobs(old + 1)
            entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
            await recovery.reenqueue_stalled(queue, entry["job_data"])

            assert await recovery.get_job_state(queue, job_id) == State.WAITING
            recovered = await recovery.dequeue(queue)
            assert recovered is not None
            assert recovered["payload"]["id"] == job_id
            await recovery.ack(queue, recovered["job_id"])
        finally:
            await recovery.drain_queue(queue)
            await recovery.close()
    finally:
        await _wait_rabbitmq()
        await metadata.redis.flushdb()
        await metadata.redis.aclose()
        await metadata.job_store.redis.aclose()
