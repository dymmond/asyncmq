import json
import time

import anyio
import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.core.stalled import stalled_recovery_scheduler
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend

pytestmark = pytest.mark.anyio

gs = settings


@pytest.fixture(params=[InMemoryBackend, RedisBackend, PostgresBackend, MongoDBBackend])
async def backend(request):
    cls = request.param
    if cls is InMemoryBackend:
        b = cls()
        yield b
    elif cls is RedisBackend:
        b = cls()
        await b.redis.flushdb()
        yield b
        await b.redis.flushdb()
    elif cls is PostgresBackend:
        await install_or_drop_postgres_backend()
        b = cls()
        await b.close()
        await b.connect()
        yield b
        await install_or_drop_postgres_backend(drop=True)
    elif cls is MongoDBBackend:
        b = cls(mongo_url="mongodb://root:mongoadmin@localhost:27017", database="test_asyncmq")
        # No explicit connect method for MongoDBBackend
        yield b
        b.store.client.drop_database("test_asyncmq")


async def test_fetch_and_reenqueue(backend):
    queue = "test"
    job_id = "jobx"
    payload = {"id": job_id, "task": "t", "args": [], "kwargs": {}, "priority": 1}

    # Seed the backend
    await backend.enqueue(queue, payload)
    # Mark active if supported
    try:
        await backend.update_job_state(queue, job_id, State.ACTIVE)
    except Exception:
        pass

    # Create stale heartbeat
    ts = time.time() - 10
    await backend.save_heartbeat(queue, job_id, ts)

    # Fetch stalled jobs
    stalled = await backend.fetch_stalled_jobs(ts + 1)
    assert any(entry["queue_name"] == queue and entry["job_data"]["id"] == job_id for entry in stalled)

    # Clear waiting queue
    if isinstance(backend, InMemoryBackend):
        backend.queues.get(queue, []).clear()
    elif isinstance(backend, RedisBackend):
        await backend.redis.delete(f"queue:{queue}:waiting")
    elif isinstance(backend, PostgresBackend):
        await backend.purge(queue, state=State.WAITING)
    elif isinstance(backend, MongoDBBackend):
        backend.queues.setdefault(queue, []).clear()

    # Reenqueue stalled job
    await backend.reenqueue_stalled(queue, payload)

    # Verify reenqueue
    if isinstance(backend, InMemoryBackend):
        assert payload in backend.queues.get(queue, [])
    elif isinstance(backend, RedisBackend):
        items = await backend.redis.zrange(f"queue:{queue}:waiting", 0, -1)
        assert any(json.loads(item)["id"] == job_id for item in items)
    elif isinstance(backend, PostgresBackend):
        state = await backend.get_job_state(queue, job_id)
        assert state == State.WAITING
    elif isinstance(backend, MongoDBBackend):
        state = await backend.get_job_state(queue, job_id)
        assert state == State.WAITING
        assert any(job["id"] == job_id for job in backend.queues.get(queue, []))


async def test_scheduler_recovery(backend, monkeypatch):
    queue = "qs"
    job_id = "js"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}

    # Seed backend
    await backend.enqueue(queue, payload)
    try:
        await backend.update_job_state(queue, job_id, State.ACTIVE)
    except Exception:
        pass
    ts = time.time() - 5
    await backend.save_heartbeat(queue, job_id, ts)

    # Shorten intervals
    monkeypatch.setattr(gs, "stalled_threshold", 1.0)
    monkeypatch.setattr(gs, "stalled_check_interval", 0.1)

    async def runner():
        async with anyio.create_task_group() as tg:
            tg.start_soon(stalled_recovery_scheduler, backend)
            await anyio.sleep(0.3)
            tg.cancel_scope.cancel()

    await runner()

    # Verify recovery
    if isinstance(backend, InMemoryBackend):
        assert await backend.get_job_state(queue, job_id) == State.WAITING
        assert any(job["id"] == job_id for job in backend.queues.get(queue, []))
    elif isinstance(backend, RedisBackend):
        items = await backend.redis.zrange(f"queue:{queue}:waiting", 0, -1)
        assert any(json.loads(item)["id"] == job_id for item in items)
    elif isinstance(backend, PostgresBackend):
        state = await backend.get_job_state(queue, job_id)
        assert state == State.WAITING
    elif isinstance(backend, MongoDBBackend):
        state = await backend.get_job_state(queue, job_id)
        assert state == State.WAITING
        assert any(job["id"] == job_id for job in backend.queues.get(queue, []))


async def test_reenqueue_stalled_releases_dequeued_active_job(backend):
    queue = "qa-active-release"
    job_id = "active-release"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}

    await backend.enqueue(queue, payload)
    raw = await backend.dequeue(queue)
    assert raw is not None

    await backend.update_job_state(queue, job_id, State.ACTIVE)
    old = time.time() - 10
    await backend.save_heartbeat(queue, job_id, old)

    stalled = await backend.fetch_stalled_jobs(old + 1)
    entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)

    await backend.reenqueue_stalled(queue, entry["job_data"])

    if isinstance(backend, InMemoryBackend):
        assert (queue, job_id) not in backend.active_jobs
        assert (queue, job_id) not in backend.heartbeats
        assert any(job["id"] == job_id for job in backend.queues.get(queue, []))
    elif isinstance(backend, RedisBackend):
        assert not await backend.redis.hexists(backend._active_key(queue), job_id)
        assert not await backend.redis.hexists(backend._job_heartbeat_key(queue), job_id)
        items = await backend.redis.zrange(f"queue:{queue}:waiting", 0, -1)
        assert any(json.loads(item)["id"] == job_id for item in items)
    elif isinstance(backend, PostgresBackend):
        assert await backend.get_job_state(queue, job_id) == State.WAITING
    elif isinstance(backend, MongoDBBackend):
        assert (queue, job_id) not in backend.heartbeats
        assert await backend.get_job_state(queue, job_id) == State.WAITING
        assert any(job["id"] == job_id for job in backend.queues.get(queue, []))


async def age_active_claim_without_heartbeat(backend, queue: str, job_id: str, active_since: float) -> None:
    if isinstance(backend, InMemoryBackend):
        payload = backend.active_jobs[(queue, job_id)]
        payload.pop("heartbeat", None)
        payload["active_since"] = active_since
        payload["updated_at"] = active_since
        backend.job_payloads[(queue, job_id)] = payload
        backend.heartbeats.pop((queue, job_id), None)
    elif isinstance(backend, RedisBackend):
        stored = await backend.job_store.load(queue, job_id)
        stored.pop("heartbeat", None)
        stored["status"] = State.ACTIVE
        stored["active_since"] = active_since
        await backend.job_store.save(queue, job_id, stored)
        await backend.redis.hset(backend._active_key(queue), job_id, str(active_since))
        await backend.redis.hdel(backend._job_heartbeat_key(queue), job_id)
    elif isinstance(backend, PostgresBackend):
        await backend.connect()
        async with backend.pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {settings.postgres_jobs_table_name}
                   SET data = (data || jsonb_build_object('active_since', $3::double precision)) - 'heartbeat',
                       status = $4,
                       updated_at = to_timestamp($3)
                 WHERE queue_name = $1
                   AND job_id = $2
                """,
                queue,
                job_id,
                active_since,
                State.ACTIVE,
            )
    elif isinstance(backend, MongoDBBackend):
        await backend.connect()
        await backend.store.collection.update_one(
            {"queue_name": queue, "job_id": job_id},
            {
                "$set": {"status": State.ACTIVE, "active_since": active_since, "updated_at": active_since},
                "$unset": {"heartbeat": ""},
            },
        )
        backend.heartbeats.pop((queue, job_id), None)


async def test_fetch_stalled_jobs_recovers_active_claim_without_first_heartbeat(backend):
    queue = "qa-active-no-first-heartbeat"
    job_id = "active-no-first-heartbeat"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}

    await backend.enqueue(queue, payload)
    raw = await backend.dequeue(queue)
    assert raw is not None

    old = time.time() - 10
    await age_active_claim_without_heartbeat(backend, queue, job_id, old)

    stalled = await backend.fetch_stalled_jobs(old + 1)
    entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
    await backend.reenqueue_stalled(queue, entry["job_data"])

    assert await backend.get_job_state(queue, job_id) == State.WAITING
    recovered = await backend.dequeue(queue)
    assert recovered is not None
    assert recovered["id"] == job_id


async def test_redis_stalled_recovery_survives_backend_restart():
    queue = "restart-redis"
    job_id = "redis-restart"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}

    producer = RedisBackend()
    recovery = RedisBackend()
    await producer.redis.flushdb()
    try:
        await producer.enqueue(queue, payload)
        raw = await producer.dequeue(queue)
        assert raw is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)

        stalled = await recovery.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
        await recovery.reenqueue_stalled(queue, entry["job_data"])

        assert not await recovery.redis.hexists(recovery._active_key(queue), job_id)
        assert not await recovery.redis.hexists(recovery._job_heartbeat_key(queue), job_id)
        recovered = await recovery.dequeue(queue)
        assert recovered and recovered["id"] == job_id
    finally:
        await producer.redis.flushdb()
        await producer.redis.aclose()
        await producer.job_store.redis.aclose()
        await recovery.redis.aclose()
        await recovery.job_store.redis.aclose()


async def test_redis_stalled_recovery_does_not_requeue_completed_snapshot():
    queue = "redis-stalled-completed-race"
    job_id = "redis-stalled-completed"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}

    producer = RedisBackend()
    recovery = RedisBackend()
    await producer.redis.flushdb()
    try:
        await producer.enqueue(queue, payload)
        active = await producer.dequeue(queue)
        assert active is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)

        stalled = await recovery.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)

        await producer.complete_active_job(queue, active, {"ok": True})
        await recovery.reenqueue_stalled(queue, entry["job_data"])

        assert await recovery.get_job_state(queue, job_id) == State.COMPLETED
        assert await recovery.get_job_result(queue, job_id) == {"ok": True}
        assert not await recovery.redis.hexists(recovery._active_key(queue), job_id)
        assert not await recovery.redis.hexists(recovery._job_heartbeat_key(queue), job_id)
        recovered = await recovery.dequeue(queue)
        assert recovered is None
    finally:
        await producer.redis.flushdb()
        await producer.redis.aclose()
        await producer.job_store.redis.aclose()
        await recovery.redis.aclose()
        await recovery.job_store.redis.aclose()


async def test_postgres_stalled_recovery_survives_backend_restart():
    queue = "restart-postgres"
    job_id = "postgres-restart"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}

    await install_or_drop_postgres_backend(drop=True)
    await install_or_drop_postgres_backend()
    producer = PostgresBackend()
    recovery = PostgresBackend()
    try:
        await producer.connect()
        await producer.enqueue(queue, payload)
        raw = await producer.dequeue(queue)
        assert raw is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)
        await producer.close()

        await recovery.connect()
        stalled = await recovery.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
        await recovery.reenqueue_stalled(queue, entry["job_data"])

        assert await recovery.get_job_state(queue, job_id) == State.WAITING
        recovered = await recovery.dequeue(queue)
        assert recovered and recovered["id"] == job_id
    finally:
        await producer.close()
        await recovery.close()
        await install_or_drop_postgres_backend(drop=True)


async def test_postgres_stalled_recovery_does_not_requeue_completed_snapshot():
    queue = "postgres-stalled-completed-race"
    job_id = "postgres-stalled-completed"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}

    await install_or_drop_postgres_backend(drop=True)
    await install_or_drop_postgres_backend()
    producer = PostgresBackend()
    recovery = PostgresBackend()
    try:
        await producer.connect()
        await recovery.connect()
        await producer.enqueue(queue, payload)
        active = await producer.dequeue(queue)
        assert active is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)

        stalled = await recovery.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)

        await producer.complete_active_job(queue, active, {"ok": True})
        await recovery.reenqueue_stalled(queue, entry["job_data"])

        assert await recovery.get_job_state(queue, job_id) == State.COMPLETED
        assert await recovery.get_job_result(queue, job_id) == {"ok": True}
        recovered = await recovery.dequeue(queue)
        assert recovered is None
    finally:
        await producer.close()
        await recovery.close()
        await install_or_drop_postgres_backend(drop=True)


async def test_mongodb_stalled_recovery_survives_backend_restart():
    queue = "restart-mongo"
    job_id = "mongo-restart"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}
    database = "test_asyncmq_restart"

    producer = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database=database)
    recovery = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database=database)
    try:
        producer.store.client.drop_database(database)
        await producer.connect()
        await recovery.connect()
        await producer.enqueue(queue, payload)
        raw = await producer.dequeue(queue)
        assert raw is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)

        stalled = await recovery.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
        await recovery.reenqueue_stalled(queue, entry["job_data"])

        assert await recovery.get_job_state(queue, job_id) == State.WAITING
        recovered = await recovery.dequeue(queue)
        assert recovered and recovered["id"] == job_id
    finally:
        producer.store.client.drop_database(database)
        producer.store.client.close()
        recovery.store.client.close()


async def test_mongodb_stalled_recovery_does_not_requeue_completed_snapshot():
    queue = "mongo-stalled-completed-race"
    job_id = "mongo-stalled-completed"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}
    database = "test_asyncmq_mongo_stalled_completed"

    producer = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database=database)
    recovery = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database=database)
    try:
        producer.store.client.drop_database(database)
        await producer.connect()
        await recovery.connect()
        await producer.enqueue(queue, payload)
        active = await producer.dequeue(queue)
        assert active is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)

        stalled = await recovery.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)

        await producer.complete_active_job(queue, active, {"ok": True})
        await recovery.reenqueue_stalled(queue, entry["job_data"])

        assert await recovery.get_job_state(queue, job_id) == State.COMPLETED
        assert await recovery.get_job_result(queue, job_id) == {"ok": True}
        recovered = await recovery.dequeue(queue)
        assert recovered is None
    finally:
        producer.store.client.drop_database(database)
        producer.store.client.close()
        recovery.store.client.close()
