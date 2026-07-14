import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

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
from asyncmq.jobs import Job

pytestmark = pytest.mark.anyio

gs = settings


def _redis_member_job_id(member: Any) -> str | None:
    raw = member.decode("utf-8") if isinstance(member, bytes) else str(member)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    return str(payload.get("id")) if isinstance(payload, dict) and payload.get("id") is not None else raw


async def _wait_for_redis_value(
    client: Any,
    key: str,
    expected: str,
    process: subprocess.Popen[str],
    *,
    timeout: float = 5.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            pytest.fail(
                f"worker process exited before {key!r} was set\n"
                f"returncode={process.returncode}\nstdout={stdout}\nstderr={stderr}"
            )
        if await client.get(key) == expected:
            return
        await anyio.sleep(0.05)
    pytest.fail(f"timed out waiting for Redis key {key!r} to equal {expected!r}")


async def _wait_for_process_exit(process: subprocess.Popen[str], *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return
        await anyio.sleep(0.05)
    process.kill()
    pytest.fail("worker process did not exit after kill")


async def _wait_for_stalled_entry(
    backend: RedisBackend,
    queue: str,
    job_id: str,
    *,
    threshold: float = 0.2,
    timeout: float = 5.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stalled = await backend.fetch_stalled_jobs(time.time() - threshold)
        for entry in stalled:
            if entry["queue_name"] == queue and entry["job_data"]["id"] == job_id:
                return entry
        await anyio.sleep(0.05)
    pytest.fail(f"timed out waiting for stalled job {job_id!r} in queue {queue!r}")


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
        assert any(_redis_member_job_id(item) == job_id for item in items)
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
        assert any(_redis_member_job_id(item) == job_id for item in items)
    elif isinstance(backend, PostgresBackend):
        state = await backend.get_job_state(queue, job_id)
        assert state == State.WAITING
    elif isinstance(backend, MongoDBBackend):
        state = await backend.get_job_state(queue, job_id)
        assert state == State.WAITING
        assert any(job["id"] == job_id for job in backend.queues.get(queue, []))


async def test_stalled_recovery_scheduler_survives_transient_fetch_failure():
    class FlakyFetchBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.fetch_calls = 0
            self.recovered: list[str] = []

        async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
            self.fetch_calls += 1
            if self.fetch_calls == 1:
                raise RuntimeError("temporary backend outage")
            return [{"queue_name": "flaky-fetch", "job_data": {"id": "recovered", "task": "tx"}}]

        async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
            self.recovered.append(str(job_data["id"]))

    backend = FlakyFetchBackend()
    async with anyio.create_task_group() as tg:
        tg.start_soon(stalled_recovery_scheduler, backend, 0.01, 0.01)
        with anyio.fail_after(1):
            while "recovered" not in backend.recovered:
                await anyio.sleep(0.01)
        tg.cancel_scope.cancel()

    assert backend.fetch_calls >= 2


async def test_stalled_recovery_scheduler_continues_after_single_reenqueue_failure():
    class PartiallyFailingBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.recovered: list[str] = []
            self.events: list[str] = []

        async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
            return [
                {"queue_name": "partial", "job_data": {"id": "bad", "task": "tx"}},
                {"queue_name": "partial", "job_data": {"id": "good", "task": "tx"}},
            ]

        async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
            job_id = str(job_data["id"])
            if job_id == "bad":
                raise RuntimeError("write failed")
            self.recovered.append(job_id)

        async def emit_event(self, event: str, data: dict[str, Any]) -> None:
            self.events.append(str(data["id"]))

    backend = PartiallyFailingBackend()
    async with anyio.create_task_group() as tg:
        tg.start_soon(stalled_recovery_scheduler, backend, 0.01, 0.01)
        with anyio.fail_after(1):
            while "good" not in backend.recovered:
                await anyio.sleep(0.01)
        tg.cancel_scope.cancel()

    assert "bad" not in backend.recovered
    assert "good" in backend.recovered
    assert "good" in backend.events


async def test_stalled_recovery_scheduler_continues_after_event_failure():
    class EventFailingBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.fetch_calls = 0
            self.recovered: list[str] = []
            self.event_attempts = 0

        async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
            self.fetch_calls += 1
            if self.fetch_calls == 1:
                return [{"queue_name": "event-failure", "job_data": {"id": "event-failed", "task": "tx"}}]
            return []

        async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
            self.recovered.append(str(job_data["id"]))

        async def emit_event(self, event: str, data: dict[str, Any]) -> None:
            self.event_attempts += 1
            raise RuntimeError("event bus unavailable")

    backend = EventFailingBackend()
    async with anyio.create_task_group() as tg:
        tg.start_soon(stalled_recovery_scheduler, backend, 0.01, 0.01)
        with anyio.fail_after(1):
            while backend.fetch_calls < 2:
                await anyio.sleep(0.01)
        tg.cancel_scope.cancel()

    assert backend.recovered == ["event-failed"]
    assert backend.event_attempts == 1


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
        assert any(_redis_member_job_id(item) == job_id for item in items)
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


async def test_memory_stalled_recovery_does_not_requeue_completed_snapshot():
    backend = InMemoryBackend()
    queue = "memory-stalled-completed-race"
    job_id = "memory-stalled-completed"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}

    await backend.enqueue(queue, payload)
    active = await backend.dequeue(queue)
    assert active is not None
    old = time.time() - 10
    await backend.save_heartbeat(queue, job_id, old)

    stalled = await backend.fetch_stalled_jobs(old + 1)
    entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)

    await backend.complete_active_job(queue, active, {"ok": True})
    await backend.reenqueue_stalled(queue, entry["job_data"])

    assert await backend.get_job_state(queue, job_id) == State.COMPLETED
    assert await backend.get_job_result(queue, job_id) == {"ok": True}
    assert await backend.dequeue(queue) is None


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


async def test_redis_worker_process_kill_releases_active_job_after_stalled_recovery(tmp_path):
    queue = "redis-worker-process-kill"
    job_id = "redis-worker-process-kill-job"
    task_id = "tests._redis_killed_worker_task"
    started_key = f"{queue}:started"
    redis_url = "redis://localhost:6379"
    payload = {"id": job_id, "task": task_id, "args": [], "kwargs": {}, "priority": 0}

    backend = RedisBackend(redis_url)
    recovery = RedisBackend(redis_url)
    await backend.redis.flushdb()

    worker_script = tmp_path / "redis_crash_worker.py"
    worker_script.write_text(
        textwrap.dedent(
            """
            import sys

            import anyio
            import redis.asyncio as redis

            from asyncmq.backends.redis import RedisBackend
            from asyncmq.conf import settings
            from asyncmq.runners import run_worker
            from asyncmq.tasks import TASK_REGISTRY


            REDIS_URL = sys.argv[1]
            QUEUE = sys.argv[2]
            STARTED_KEY = sys.argv[3]
            TASK_ID = sys.argv[4]


            async def killed_worker_task() -> None:
                client = redis.from_url(REDIS_URL, decode_responses=True)
                try:
                    await client.set(STARTED_KEY, "1")
                finally:
                    await client.aclose()
                await anyio.sleep(60)


            async def main() -> None:
                TASK_REGISTRY.clear()
                TASK_REGISTRY[TASK_ID] = {"func": killed_worker_task}
                settings.backend = RedisBackend(REDIS_URL)
                settings.enable_stalled_check = True
                settings.stalled_threshold = 0.2
                settings.stalled_check_interval = 0.05
                await run_worker(
                    QUEUE,
                    backend=settings.backend,
                    concurrency=1,
                    rate_limit=None,
                    repeatables=None,
                    scan_interval=0.05,
                )


            if __name__ == "__main__":
                anyio.run(main)
            """
        )
    )

    project_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    process = subprocess.Popen(
        [sys.executable, str(worker_script), redis_url, queue, started_key, task_id],
        cwd=project_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        await backend.enqueue(queue, payload)

        await _wait_for_redis_value(backend.redis, started_key, "1", process)
        assert await backend.get_job_state(queue, job_id) == State.ACTIVE
        assert await backend.redis.hexists(backend._active_key(queue), job_id)

        process.kill()
        await _wait_for_process_exit(process)

        stalled = await _wait_for_stalled_entry(recovery, queue, job_id)
        await recovery.reenqueue_stalled(queue, stalled["job_data"])

        assert await recovery.get_job_state(queue, job_id) == State.WAITING
        assert not await recovery.redis.hexists(recovery._active_key(queue), job_id)
        assert not await recovery.redis.hexists(recovery._job_heartbeat_key(queue), job_id)

        recovered = await recovery.dequeue(queue)
        assert recovered is not None
        assert recovered["id"] == job_id
        assert recovered["active_since"] != stalled["job_data"]["active_since"]
    finally:
        if process.poll() is None:
            process.kill()
            await _wait_for_process_exit(process)
        process.communicate(timeout=1)
        await backend.redis.flushdb()
        await backend.redis.aclose()
        await backend.job_store.redis.aclose()
        await recovery.redis.aclose()
        await recovery.job_store.redis.aclose()


async def test_redis_stalled_recovery_preserves_waiting_priority_order():
    queue = "redis-stalled-priority-order"
    high_id = "redis-stalled-high-priority"
    low_id = "redis-waiting-low-priority"
    high_priority = {"id": high_id, "task": "tx", "args": [], "kwargs": {}, "priority": 1}
    low_priority = {"id": low_id, "task": "tx", "args": [], "kwargs": {}, "priority": 10}

    backend = RedisBackend()
    await backend.redis.flushdb()
    try:
        await backend.enqueue(queue, high_priority)
        active = await backend.dequeue(queue)
        assert active is not None
        assert active["id"] == high_id

        await backend.enqueue(queue, low_priority)
        old = time.time() - 10
        await backend.save_heartbeat(queue, high_id, old)

        stalled = await backend.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == high_id)
        await backend.reenqueue_stalled(queue, entry["job_data"])

        first = await backend.dequeue(queue)
        assert first is not None
        assert first["id"] == high_id
    finally:
        await backend.redis.flushdb()
        await backend.redis.aclose()
        await backend.job_store.redis.aclose()


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


async def test_redis_stale_completion_does_not_clear_recovered_active_claim():
    queue = "redis-stalled-stale-complete"
    job_id = "redis-stale-complete"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}

    producer = RedisBackend()
    recovery = RedisBackend()
    await producer.redis.flushdb()
    try:
        await producer.enqueue(queue, payload)
        stale_active = await producer.dequeue(queue)
        assert stale_active is not None
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)

        stalled = await recovery.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
        await recovery.reenqueue_stalled(queue, entry["job_data"])

        recovered = await recovery.dequeue(queue)
        assert recovered is not None
        assert recovered["id"] == job_id
        assert recovered["active_since"] != stale_active["active_since"]

        await producer.complete_active_job(queue, stale_active, {"stale": True})

        assert await recovery.get_job_state(queue, job_id) == State.ACTIVE
        assert await recovery.get_job_result(queue, job_id) is None
        assert await recovery.redis.hexists(recovery._active_key(queue), job_id)

        await recovery.complete_active_job(queue, recovered, {"fresh": True})

        assert await recovery.get_job_state(queue, job_id) == State.COMPLETED
        assert await recovery.get_job_result(queue, job_id) == {"fresh": True}
        assert not await recovery.redis.hexists(recovery._active_key(queue), job_id)
    finally:
        await producer.redis.flushdb()
        await producer.redis.aclose()
        await producer.job_store.redis.aclose()
        await recovery.redis.aclose()
        await recovery.job_store.redis.aclose()


async def test_redis_worker_payload_preserves_active_claim_token_for_stale_completion():
    queue = "redis-stalled-worker-token"
    job_id = "redis-worker-token"
    payload = {"id": job_id, "task": "tx", "args": [], "kwargs": {}, "priority": 0}

    producer = RedisBackend()
    recovery = RedisBackend()
    await producer.redis.flushdb()
    try:
        await producer.enqueue(queue, payload)
        stale_active = await producer.dequeue(queue)
        assert stale_active is not None
        stale_worker_payload = Job.from_dict(stale_active).to_dict()
        assert stale_worker_payload["active_since"] == stale_active["active_since"]

        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)

        stalled = await recovery.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
        await recovery.reenqueue_stalled(queue, entry["job_data"])

        recovered = await recovery.dequeue(queue)
        assert recovered is not None
        assert recovered["active_since"] != stale_worker_payload["active_since"]

        await producer.complete_active_job(queue, stale_worker_payload, {"stale": True})

        assert await recovery.get_job_state(queue, job_id) == State.ACTIVE
        assert await recovery.get_job_result(queue, job_id) is None
        assert await recovery.redis.hexists(recovery._active_key(queue), job_id)
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
