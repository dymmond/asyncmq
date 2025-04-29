import json
import time

import anyio
import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.conf import settings as gs
from asyncmq.core.enums import State
from asyncmq.core.stalled import stalled_recovery_scheduler
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend

pytestmark = pytest.mark.anyio

@ pytest.fixture(params=[InMemoryBackend, RedisBackend, PostgresBackend, MongoDBBackend])
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
        assert payload in backend.queues.get(queue, [])

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
    monkeypatch.setattr(gs, 'stalled_threshold', 1.0)
    monkeypatch.setattr(gs, 'stalled_check_interval', 0.1)

    async def runner():
        async with anyio.create_task_group() as tg:
            tg.start_soon(stalled_recovery_scheduler, backend)
            await anyio.sleep(0.3)
            tg.cancel_scope.cancel()

    await runner()

    # Verify recovery
    if isinstance(backend, InMemoryBackend):
        assert payload in backend.queues.get(queue, [])
    elif isinstance(backend, RedisBackend):
        items = await backend.redis.zrange(f"queue:{queue}:waiting", 0, -1)
        assert any(json.loads(item)["id"] == job_id for item in items)
    elif isinstance(backend, PostgresBackend):
        state = await backend.get_job_state(queue, job_id)
        assert state == State.WAITING
    elif isinstance(backend, MongoDBBackend):
        assert payload in backend.queues.get(queue, [])
