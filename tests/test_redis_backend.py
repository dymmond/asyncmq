import asyncio
import json
import time

import anyio
import pytest
import redis.asyncio as async_redis

from asyncmq import Worker
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.enums import State
from asyncmq.jobs import Job
from asyncmq.logging import logger
from asyncmq.stores.redis_store import COMPRESSED_PAYLOAD_PREFIX

pytestmark = pytest.mark.asyncio


async def test_create_with_url(redis):
    backend = RedisBackend()
    assert await backend.redis.client().ping()


async def test_url_backend_uses_blocking_pool_for_connection_backpressure(redis):
    backend = RedisBackend(max_connections=1, pool_timeout=2.0)
    queue = "redis-blocking-pool"
    assert isinstance(backend.redis.connection_pool, async_redis.BlockingConnectionPool)

    for index in range(20):
        job = Job(task_id="redis.pool", args=[], kwargs={}, job_id=f"pool-{index}")
        await backend.enqueue(queue, job.to_dict())

    async def claim_and_complete() -> None:
        payload = await backend.dequeue(queue)
        if payload is not None:
            await backend.complete_active_job(queue, payload, {"ok": True})

    async with anyio.create_task_group() as tg:
        for _ in range(20):
            tg.start_soon(claim_and_complete)

    completed = [await backend.get_job_state(queue, f"pool-{index}") for index in range(20)]
    assert completed == [State.COMPLETED] * 20


async def test_create_with_client(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    assert backend.redis == redis


async def test_enqueue_and_dequeue(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.enqueue", args=[], kwargs={})
    job_payload = job.to_dict()  # Capture payload to compare later

    await backend.enqueue("test", job_payload)
    waiting_key = backend._waiting_key("test")
    zmembers = await redis.zrange(waiting_key, 0, -1)

    if zmembers:
        try:
            decoded = [json.loads(item) for item in zmembers]
            logger.info(f"Redis waiting‐set content (decoded): {decoded}")
        except json.JSONDecodeError:
            logger.info("Could not decode sorted‐set content as JSON.")

    result = await backend.dequeue("test")
    assert result["id"] == job.id


async def test_enqueue_stores_waiting_member_as_job_id(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-waiting-id-member"
    body = "x" * 256_000
    job = Job(task_id="redis.large", args=[body], kwargs={}, job_id="redis-large-id")

    await backend.enqueue(queue, job.to_dict())

    members = await redis.zrange(backend._waiting_key(queue), 0, -1)
    assert members == [job.id]
    assert body not in members[0]

    stored = await backend.get_job(queue, job.id)
    assert stored is not None
    assert stored["args"] == [body]

    dequeued = await backend.dequeue(queue)
    assert dequeued is not None
    assert dequeued["id"] == job.id
    assert dequeued["args"] == [body]


async def test_redis_large_payload_uses_compressed_side_payload(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-compressed-payload"
    body = "x" * 256_000
    job = Job(task_id="redis.large.compressed", args=[body], kwargs={"marker": body}, job_id="redis-large-compressed")

    await backend.enqueue(queue, job.to_dict())

    canonical = await redis.get(backend._job_store_key(queue, job.id))
    payload = await redis.get(backend.job_store._payload_key(queue, job.id))

    assert canonical is not None
    assert payload is not None
    assert body not in canonical
    assert payload.startswith(COMPRESSED_PAYLOAD_PREFIX)

    canonical_data = json.loads(canonical)
    assert "args" not in canonical_data
    assert "kwargs" not in canonical_data
    assert canonical_data["id"] == job.id

    stored = await backend.get_job(queue, job.id)
    assert stored is not None
    assert stored["args"] == [body]
    assert stored["kwargs"] == {"marker": body}

    dequeued = await backend.dequeue(queue)
    assert dequeued is not None
    assert dequeued["id"] == job.id
    assert dequeued["args"] == [body]
    assert dequeued["kwargs"] == {"marker": body}
    assert dequeued["status"] == State.ACTIVE


async def test_redis_small_payload_stays_inline_without_side_payload(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-small-inline-payload"
    job = Job(task_id="redis.small.inline", args=["small"], kwargs={"marker": "small"}, job_id="redis-small-inline")

    await backend.enqueue(queue, job.to_dict())

    canonical = await redis.get(backend._job_store_key(queue, job.id))
    payload = await redis.get(backend.job_store._payload_key(queue, job.id))

    assert canonical is not None
    assert payload is None
    canonical_data = json.loads(canonical)
    assert canonical_data["args"] == ["small"]
    assert canonical_data["kwargs"] == {"marker": "small"}
    assert await backend.get_job(queue, job.id) == {**canonical_data, "status": State.WAITING}


async def test_redis_dequeue_does_not_reload_inline_small_payload(redis, monkeypatch):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-small-inline-dequeue"
    job = Job(task_id="redis.small.inline.dequeue", args=["small"], kwargs={}, job_id="redis-small-inline-dequeue")

    await backend.enqueue(queue, job.to_dict())

    async def fail_load(*args, **kwargs):
        raise AssertionError("inline dequeue should not reload the canonical job payload")

    monkeypatch.setattr(backend.job_store, "load", fail_load)

    payload = await backend.dequeue(queue)

    assert payload is not None
    assert payload["id"] == job.id
    assert payload["args"] == ["small"]
    assert payload["kwargs"] == {}


async def test_redis_job_store_loads_legacy_inline_payload(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-legacy-inline-payload"
    job = Job(task_id="redis.legacy.inline", args=["legacy"], kwargs={}, job_id="redis-legacy-inline").to_dict()

    await redis.set(backend._job_store_key(queue, job["id"]), backend._json_serializer.to_json(job))
    await redis.sadd(backend._job_store_ids_key(queue), job["id"])

    stored = await backend.job_store.load(queue, job["id"])
    assert stored == job


async def test_dequeue_accepts_legacy_json_waiting_member(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-legacy-json-waiting"
    payload = {
        **Job(task_id="redis.legacy", args=["legacy"], kwargs={}, job_id="legacy-json-waiting").to_dict(),
        "status": State.WAITING,
    }

    await redis.zadd(backend._waiting_key(queue), {backend._json_serializer.to_json(payload): 1})

    dequeued = await backend.dequeue(queue)
    assert dequeued is not None
    assert dequeued["id"] == payload["id"]
    assert dequeued["status"] == State.ACTIVE
    assert await backend.get_job_state(queue, payload["id"]) == State.ACTIVE


async def test_same_priority_jobs_preserve_fifo_order(redis):
    backend = RedisBackend()
    first = Job(task_id="redis.first", args=[], kwargs={}, priority=5)
    second = Job(task_id="redis.second", args=[], kwargs={}, priority=5)

    await backend.enqueue("test", first.to_dict())
    await backend.enqueue("test", second.to_dict())

    first_out = await backend.dequeue("test")
    second_out = await backend.dequeue("test")

    assert first_out is not None
    assert second_out is not None
    assert first_out["id"] == first.id
    assert second_out["id"] == second.id


async def test_job_state_tracking(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.state", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    await backend.update_job_state("test", job.id, State.ACTIVE)
    state = await backend.get_job_state("test", job.id)
    assert state == State.ACTIVE


async def test_job_result_handling(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.result", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    await backend.save_job_result("test", job.id, 9876)
    result = await backend.get_job_result("test", job.id)
    assert result == 9876


async def test_complete_active_job_updates_result_and_releases_redis_ownership(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-lifecycle-complete"
    job = Job(task_id="redis.lifecycle.complete", args=[], kwargs={}, job_id="j-complete")

    await backend.enqueue(queue, job.to_dict())
    payload = await backend.dequeue(queue)
    await backend.save_heartbeat(queue, job.id, time.time())

    assert payload is not None
    assert await redis.hexists(backend._active_key(queue), job.id)
    assert await redis.hexists(backend._job_heartbeat_key(queue), job.id)

    await backend.complete_active_job(queue, payload, {"ok": True})

    assert await backend.get_job_state(queue, job.id) == State.COMPLETED
    assert await backend.get_job_result(queue, job.id) == {"ok": True}
    assert not await redis.hexists(backend._active_key(queue), job.id)
    assert not await redis.hexists(backend._job_heartbeat_key(queue), job.id)
    assert await redis.zcard(backend._waiting_key(queue)) == 0
    assert await redis.zcard(backend._delayed_key(queue)) == 0


async def test_redis_claim_and_complete_do_not_rewrite_large_canonical_payload(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-lifecycle-small-metadata"
    body = "x" * 256_000
    job = Job(task_id="redis.lifecycle.metadata", args=[body], kwargs={}, job_id="j-small-metadata")

    await backend.enqueue(queue, job.to_dict())
    canonical_before = await redis.get(backend._job_store_key(queue, job.id))

    payload = await backend.dequeue(queue)
    canonical_after_claim = await redis.get(backend._job_store_key(queue, job.id))

    assert payload is not None
    assert canonical_after_claim == canonical_before
    assert await backend.get_job_state(queue, job.id) == State.ACTIVE

    await backend.complete_active_job(queue, payload, {"ok": True})
    canonical_after_complete = await redis.get(backend._job_store_key(queue, job.id))
    hydrated = await backend.get_job(queue, job.id)

    assert canonical_after_complete == canonical_before
    assert await backend.get_job_state(queue, job.id) == State.COMPLETED
    assert await backend.get_job_result(queue, job.id) == {"ok": True}
    assert hydrated is not None
    assert hydrated["status"] == State.COMPLETED
    assert hydrated["result"] == {"ok": True}
    assert hydrated["args"] == [body]


async def test_redis_dequeue_claims_without_client_side_pop_or_pipeline(redis, monkeypatch):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-claim-script-hot-path"
    job = Job(task_id="redis.claim.script", args=[], kwargs={}, job_id="j-claim-script")

    await backend.enqueue(queue, job.to_dict())

    async def fail_pop_script(*args, **kwargs):
        raise AssertionError("dequeue should use the Redis claim script, not pop_script")

    def fail_pipeline(*args, **kwargs):
        raise AssertionError("dequeue should not perform client-side claim pipelines")

    monkeypatch.setattr(backend, "pop_script", fail_pop_script)
    monkeypatch.setattr(backend.redis, "pipeline", fail_pipeline)

    payload = await backend.dequeue(queue)

    assert payload is not None
    assert payload["id"] == job.id
    assert payload["status"] == State.ACTIVE
    assert await redis.hget(backend._active_key(queue), job.id) == repr(payload["active_since"])
    assert await redis.zcard(backend._waiting_key(queue)) == 0


async def test_complete_active_job_preserves_unrelated_redis_waiting_backlog(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-lifecycle-complete-backlog"
    active_job = Job(task_id="redis.lifecycle.complete.backlog", args=[], kwargs={}, job_id="j-complete-backlog")

    await backend.enqueue(queue, active_job.to_dict())
    payload = await backend.dequeue(queue)
    assert payload is not None

    backlog = {
        backend._json_serializer.to_json(
            Job(task_id="redis.lifecycle.backlog", args=[index], kwargs={}, job_id=f"backlog-{index}").to_dict()
        ): index
        for index in range(500)
    }
    await redis.zadd(backend._waiting_key(queue), backlog)

    await backend.complete_active_job(queue, payload, {"ok": True})

    assert await backend.get_job_state(queue, active_job.id) == State.COMPLETED
    assert await redis.zcard(backend._waiting_key(queue)) == len(backlog)


async def test_cancelled_active_job_completion_does_not_overwrite_redis_marker(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-lifecycle-cancel-active"
    job = Job(task_id="redis.lifecycle.cancel", args=[], kwargs={}, job_id="j-cancel-active")

    await backend.enqueue(queue, job.to_dict())
    payload = await backend.dequeue(queue)

    assert payload is not None
    assert await backend.cancel_job(queue, job.id) is True
    assert await backend.is_job_cancelled(queue, job.id) is True
    assert await backend.get_job_state(queue, job.id) == "cancelled"

    await backend.complete_active_job(queue, payload, {"ok": True})

    assert await backend.get_job_state(queue, job.id) == "cancelled"
    assert await backend.get_job_result(queue, job.id) is None


async def test_cancelled_waiting_job_is_not_listed_as_redis_waiting(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-cancel-waiting"
    job = Job(task_id="redis.cancel.waiting", args=[], kwargs={}, job_id="j-cancel-waiting")

    await backend.enqueue(queue, job.to_dict())

    assert await backend.cancel_job(queue, job.id) is True

    assert await backend.get_job_state(queue, job.id) == "cancelled"
    waiting = await backend.list_jobs(queue, State.WAITING)
    assert all(item["id"] != job.id for item in waiting)


async def test_cancel_job_removes_id_waiting_member(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-cancel-id-waiting"
    job = Job(task_id="redis.cancel.id", args=[], kwargs={}, job_id="redis-cancel-id")

    await backend.enqueue(queue, job.to_dict())

    assert await redis.zrange(backend._waiting_key(queue), 0, -1) == [job.id]
    assert await backend.cancel_job(queue, job.id) is True
    assert await redis.zrange(backend._waiting_key(queue), 0, -1) == []
    assert await backend.get_job_state(queue, job.id) == "cancelled"


async def test_dequeue_skips_stale_cancelled_redis_waiting_member(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-cancel-stale-waiting"
    job = Job(task_id="redis.cancel.stale", args=[], kwargs={}, job_id="j-cancel-stale")

    await backend.enqueue(queue, job.to_dict())
    raw_waiting = await redis.zrange(backend._waiting_key(queue), 0, -1, withscores=True)

    assert len(raw_waiting) == 1

    raw_member, score = raw_waiting[0]

    assert await backend.cancel_job(queue, job.id) is True
    assert await backend.get_job_state(queue, job.id) == "cancelled"

    await redis.zadd(backend._waiting_key(queue), {raw_member: score})

    assert await backend.dequeue(queue) is None
    assert await backend.get_job_state(queue, job.id) == "cancelled"
    assert not await redis.zrange(backend._waiting_key(queue), 0, -1)
    assert not await redis.hexists(backend._active_key(queue), job.id)


async def test_retry_active_job_moves_redis_job_to_delayed_atomically(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-lifecycle-retry"
    job = Job(task_id="redis.lifecycle.retry", args=[], kwargs={}, job_id="j-retry")

    await backend.enqueue(queue, job.to_dict())
    payload = await backend.dequeue(queue)
    await backend.save_heartbeat(queue, job.id, time.time())

    assert payload is not None

    run_at = time.time() + 60
    payload = {**payload, "retries": 1}
    await backend.retry_active_job(queue, payload, run_at)

    assert await backend.get_job_state(queue, job.id) == State.DELAYED
    assert not await redis.hexists(backend._active_key(queue), job.id)
    assert not await redis.hexists(backend._job_heartbeat_key(queue), job.id)

    delayed = await redis.zrange(backend._delayed_key(queue), 0, -1, withscores=True)
    assert len(delayed) == 1
    delayed_payload = json.loads(delayed[0][0])
    assert delayed_payload["id"] == job.id
    assert delayed_payload["status"] == State.DELAYED
    assert delayed[0][1] == pytest.approx(run_at)


async def test_fail_active_job_moves_redis_job_to_dlq_atomically(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-lifecycle-fail"
    job = Job(task_id="redis.lifecycle.fail", args=[], kwargs={}, job_id="j-fail")

    await backend.enqueue(queue, job.to_dict())
    payload = await backend.dequeue(queue)
    await backend.save_heartbeat(queue, job.id, time.time())

    assert payload is not None

    await backend.fail_active_job(queue, {**payload, "status": State.FAILED})

    assert await backend.get_job_state(queue, job.id) == State.FAILED
    assert not await redis.hexists(backend._active_key(queue), job.id)
    assert not await redis.hexists(backend._job_heartbeat_key(queue), job.id)

    dlq = await redis.zrange(backend._dlq_key(queue), 0, -1)
    assert len(dlq) == 1
    dlq_payload = json.loads(dlq[0])
    assert dlq_payload["id"] == job.id
    assert dlq_payload["status"] == State.FAILED


async def test_enqueue_delayed_and_due(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.delayed", args=[], kwargs={})
    run_at = time.time() + 0.3
    await backend.enqueue_delayed("test", job.to_dict(), run_at)

    await asyncio.sleep(0.6)  # Increased delay
    due = await backend.get_due_delayed("test")
    print("DUE JOBS:", due)

    assert any(j.get("id") == job.id for j in due)


async def test_promote_due_delayed_moves_redis_job_to_waiting_atomically(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-promote-delayed"
    job = Job(task_id="redis.promote", args=[], kwargs={}, job_id="redis-promote", priority=1)

    await backend.enqueue_delayed(queue, job.to_dict(), time.time() - 1)
    promoted = await backend.promote_due_delayed(queue)

    assert [item["id"] for item in promoted] == [job.id]
    assert await redis.zcard(backend._delayed_key(queue)) == 0
    assert await redis.zcard(backend._waiting_key(queue)) == 1
    assert await redis.zrange(backend._waiting_key(queue), 0, -1) == [job.id]
    stored = await backend.get_job(queue, job.id)
    assert stored["status"] == State.WAITING
    dequeued = await backend.dequeue(queue)
    assert dequeued["id"] == job.id


async def test_resolved_dependency_keeps_redis_delayed_child_delayed(redis):
    backend = RedisBackend(redis_url_or_client=redis)
    queue = "redis-delayed-child-deps"
    parent_id = "redis-delayed-parent"
    run_at = time.time() + 60
    child = Job(
        task_id="redis.delayed.child",
        args=[],
        kwargs={},
        job_id="redis-delayed-child",
        priority=1,
        depends_on=[parent_id],
    )

    payload = {**child.to_dict(), "delay_until": run_at}
    await backend.enqueue_delayed(queue, payload, run_at)
    await backend.add_dependencies(queue, payload)

    await backend.resolve_dependency(queue, parent_id)

    assert await backend.dequeue(queue) is None
    assert await redis.zcard(backend._waiting_key(queue)) == 0
    assert await redis.zcard(backend._delayed_key(queue)) == 1

    stored = await backend.get_job(queue, child.id)
    assert stored["status"] == State.DELAYED
    assert "depends_on" not in stored

    delayed = await redis.zrange(backend._delayed_key(queue), 0, -1)
    delayed_payload = json.loads(delayed[0])
    assert delayed_payload["id"] == child.id
    assert delayed_payload["status"] == State.DELAYED
    assert "depends_on" not in delayed_payload

    await redis.zadd(backend._delayed_key(queue), {delayed[0]: time.time() - 1})
    promoted = await backend.promote_due_delayed(queue)

    assert [item["id"] for item in promoted] == [child.id]
    assert await redis.zcard(backend._delayed_key(queue)) == 0
    dequeued = await backend.dequeue(queue)
    assert dequeued is not None
    assert dequeued["id"] == child.id


async def test_move_to_dlq(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.dlq", args=[], kwargs={})
    await backend.move_to_dlq("test", job.to_dict())
    state = await backend.get_job_state("test", job.id)
    assert state == State.FAILED


async def test_remove_delayed(redis):
    backend = RedisBackend()
    job = Job(task_id="redis.remove_delayed", args=[], kwargs={})
    run_at = time.time() + 1.0
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    assert await backend.remove_delayed("test", job.id)
    assert not await backend.remove_delayed("test", job.id)
    delayed = await backend.get_due_delayed("test")
    assert all(j["id"] != job.id for j in delayed)


@pytest.mark.parametrize("state", ["waiting", "delayed", "failed"])
async def test_list_jobs_by_state(state):
    backend = RedisBackend()
    queue = "test-queue"
    job = Job(task_id="test.task", args=[], kwargs={})

    if state == "waiting":
        await backend.enqueue(queue, job.to_dict())
    elif state == "delayed":
        delayed_job = job.to_dict()
        await backend.enqueue_delayed(queue, delayed_job, run_at=9999999999)
    elif state == "failed":
        await backend.enqueue(queue, job.to_dict())
        await backend.move_to_dlq(queue, job.to_dict())

    jobs = await backend.list_jobs(queue, state)
    print("Returned jobs:", jobs)
    assert isinstance(jobs, list)
    assert any(j.get("task") == "test.task" for j in jobs)


@pytest.mark.parametrize("state", ["waiting", "delayed", "failed"])
async def test_list_jobs_empty_queue(state):
    backend = RedisBackend()
    jobs = await backend.list_jobs("empty-queue", state)
    assert isinstance(jobs, list)
    assert len(jobs) == 0


async def test_list_jobs_filters_correctly():
    backend = RedisBackend()
    queue = "filter-test"
    job1 = Job(task_id="waiting.job", args=[], kwargs={})
    job2 = Job(task_id="delayed.job", args=[], kwargs={})
    job3 = Job(task_id="failed.job", args=[], kwargs={})

    await backend.enqueue(queue, job1.to_dict())
    await backend.enqueue_delayed(queue, job2.to_dict(), run_at=9999999999)
    await backend.enqueue(queue, job3.to_dict())
    await backend.move_to_dlq(queue, job3.to_dict())

    waiting = await backend.list_jobs(queue, "waiting")
    delayed = await backend.list_jobs(queue, "delayed")
    failed = await backend.list_jobs(queue, "failed")

    assert all(j["task"] == "waiting.job" for j in waiting)
    assert all(j["task"] == "delayed.job" for j in delayed)
    assert all(j["task"] == "failed.job" for j in failed)


async def test_queue_list_is_populated_when_starting_a_worker():
    from asyncmq.conf import settings

    backend = RedisBackend()

    settings.backend = backend

    queue = "test_queue"
    worker = Worker(queue, heartbeat_interval=0.1)

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker._run_with_scope)
        await anyio.sleep(0.15)

        # Now list queues — it should include the queue due to heartbeats
        queues = await backend.list_queues()
        assert queue in queues

        # Stop the worker to clean up
        tg.cancel_scope.cancel()
