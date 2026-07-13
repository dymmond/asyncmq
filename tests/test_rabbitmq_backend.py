import asyncio
import time

import aio_pika
import pytest
import pytest_asyncio

from asyncmq.backends.rabbitmq import RabbitMQBackend
from asyncmq.core.enums import State
from asyncmq.stores.redis_store import RedisJobStore

pytestmark = pytest.mark.asyncio

RABBIT_URL = "amqp://guest:guest@localhost/"
RABBIT_TEST_QUEUES = (
    "test_q",
    "test_q.dlq",
    "test_q_priority_0_9",
    "test_q_promote_delayed",
    "test_q_stalled_restart",
    "test_q_stalled_without_first_heartbeat",
    "test_list_state",
    "test_list_state.dlq",
    "test_filters",
    "test_filters.dlq",
    "test_case",
    "wrong_status",
)


async def delete_rabbitmq_test_queues() -> None:
    connection = await aio_pika.connect_robust(RABBIT_URL)
    try:
        channel = await connection.channel()
        for queue_name in RABBIT_TEST_QUEUES:
            await channel.queue_delete(queue_name, if_unused=False, if_empty=False)
    finally:
        await connection.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_rabbitmq_test_queues():
    await delete_rabbitmq_test_queues()
    yield
    await delete_rabbitmq_test_queues()


@pytest_asyncio.fixture(scope="function")
async def redis_store(redis):
    # instantiate RedisJobStore and override its client to use pytest-redis fixture
    store = RedisJobStore(redis_url=None)
    store.redis = redis
    await redis.flushall()
    return store


@pytest_asyncio.fixture(scope="function")
async def backend(redis_store):
    # Create RabbitMQ backend with Redis-based metadata store
    backend = RabbitMQBackend(rabbit_url=RABBIT_URL, job_store=redis_store, max_priority=None)
    yield backend
    # Cleanup after test
    await backend.close()


async def test_enqueue_and_dequeue_immediate(backend, redis_store):
    payload = {"id": "j1", "task": "do_something"}
    jid = await backend.enqueue("test_q", payload)
    assert jid == "j1"

    job = await backend.dequeue("test_q")
    assert job is not None
    assert job["payload"]["task"] == "do_something"

    await backend.update_job_state("test_q", "j1", "completed")
    await backend.ack("test_q", job["job_id"])
    state = await redis_store.load("test_q", "j1")
    assert state["status"] == "completed"


async def test_dequeue_respects_priority_then_fifo(redis_store):
    queue = "test_q_priority_0_9"
    backend = RabbitMQBackend(rabbit_url=RABBIT_URL, job_store=redis_store)
    await backend.drain_queue(queue)

    try:
        low = {"id": "rabbit-low", "task": "low", "priority": 10}
        first = {"id": "rabbit-first", "task": "first", "priority": 1}
        second = {"id": "rabbit-second", "task": "second", "priority": 1}

        await backend.enqueue(queue, low)
        await backend.enqueue(queue, first)
        await backend.enqueue(queue, second)

        assert (await backend.dequeue(queue))["payload"]["id"] == "rabbit-first"
        assert (await backend.dequeue(queue))["payload"]["id"] == "rabbit-second"
        assert (await backend.dequeue(queue))["payload"]["id"] == "rabbit-low"

        await backend.drain_queue(queue)
    finally:
        await backend.close()


async def test_move_to_dlq(backend):
    payload = {"id": "j2", "task": "fail_me"}
    await backend.move_to_dlq("test_q", payload)
    dlq_job = await backend.dequeue("test_q.dlq")
    assert dlq_job is not None
    assert dlq_job["payload"]["task"] == "fail_me"
    await backend.ack("test_q.dlq", dlq_job["job_id"])


async def test_delayed_jobs(backend):
    now = time.time()
    payload = {"id": "j3", "task": "delayed"}
    await backend.enqueue_delayed("test_q", payload, run_at=now - 1)

    due = await backend.get_due_delayed("test_q")
    assert any(di.job_id == "j3" and di.payload["task"] == "delayed" for di in due)


async def test_promote_due_delayed_publishes_rabbitmq_job_before_clearing_delayed_metadata(backend):
    queue = "test_q_promote_delayed"
    await backend.drain_queue(queue)
    payload = {"id": "rabbit-promote", "task": "promote", "priority": 1}

    await backend.enqueue_delayed(queue, payload, run_at=time.time() - 1)
    promoted = await backend.promote_due_delayed(queue)

    assert [item["id"] for item in promoted] == ["rabbit-promote"]
    assert await backend.list_delayed(queue) == []
    assert await backend.get_job_state(queue, "rabbit-promote") == State.WAITING
    dequeued = await backend.dequeue(queue)
    assert dequeued["payload"]["id"] == "rabbit-promote"


async def test_list_and_remove_delayed(backend):
    run_at = time.time() + 10
    payload = {"id": "j4", "task": "later"}
    await backend.enqueue_delayed("test_q", payload, run_at=run_at)
    lst = await backend.list_delayed("test_q")
    assert any(di.job_id == "j4" for di in lst)

    assert await backend.remove_delayed("test_q", "j4")
    assert not await backend.remove_delayed("test_q", "j4")
    lst2 = await backend.list_delayed("test_q")
    assert not any(di.job_id == "j4" for di in lst2)


async def test_repeatable_jobs(backend):
    payload = {"id": "j5", "task": "repeat"}
    rid = await backend.enqueue_repeatable("test_q", payload, interval=5)
    reps = await backend.list_repeatables("test_q")
    assert any(r.job_def["task"] == "repeat" for r in reps)

    await backend.pause_repeatable("test_q", {"id": rid})
    paused = await backend.list_repeatables("test_q")
    assert any(r.paused for r in paused)

    next_run = await backend.resume_repeatable("test_q", {"id": rid})
    assert next_run > time.time()

    await backend.remove_repeatable("test_q", rid)
    final = await backend.list_repeatables("test_q")
    assert not any(r.job_def.get("id") == rid for r in final)


async def test_atomic_add_flow_and_dependencies(backend):
    jobs = [{"id": "p1", "task": "A"}, {"id": "c1", "task": "B"}]
    created = await backend.atomic_add_flow("test_q", jobs, [("p1", "c1")])
    assert created == ["p1", "c1"]

    # Only parent is dequeued initially
    first = await backend.dequeue("test_q")
    assert first and first["payload"]["id"] == "p1"

    await backend.resolve_dependency("test_q", "p1")
    second = await backend.dequeue("test_q")
    assert second and second["payload"]["id"] == "c1"


async def test_cancel_remove_retry_and_is_cancelled(backend, redis_store):
    payload = {"id": "j6", "task": "todo"}
    await backend.enqueue("test_q", payload)
    await backend.cancel_job("test_q", "j6")
    assert await backend.is_job_cancelled("test_q", "j6")

    await backend.remove_job("test_q", "j6")
    assert not await backend.is_job_cancelled("test_q", "j6")

    assert not await backend.retry_job("test_q", "x1")
    await backend.enqueue("test_q", payload)
    assert await backend.retry_job("test_q", "j6")


async def test_retry_job_publishes_clean_waiting_payload(backend, redis_store):
    payload = {
        "id": "rabbit-retry-clean",
        "task": "retry",
        "status": State.FAILED,
        "result": "old",
        "last_error": "old failure",
        "error_traceback": "old traceback",
    }
    await redis_store.save("test_q", "rabbit-retry-clean", {"id": "rabbit-retry-clean", "payload": payload})

    assert await backend.retry_job("test_q", "rabbit-retry-clean")

    message = await backend.dequeue("test_q")
    assert message["payload"]["id"] == "rabbit-retry-clean"
    assert message["payload"]["status"] == State.WAITING
    assert "result" not in message["payload"]
    assert "last_error" not in message["payload"]
    assert "error_traceback" not in message["payload"]


async def test_worker_registration_and_listing(backend):
    await backend.register_worker("w1", "test_q", 2, time.time())
    workers = await backend.list_workers()
    assert any(w.id == "w1" and w.queue == "test_q" for w in workers)

    await backend.deregister_worker("w1")
    workers2 = await backend.list_workers()
    assert not any(w.id == "w1" for w in workers2)


async def test_queue_stats_and_drain(backend):
    for i in range(3):
        await backend.enqueue("test_q", {"id": f"s{i}"})
    stats = await backend.queue_stats("test_q")
    assert stats["message_count"] >= 3
    assert {"waiting", "delayed", "failed"} <= set(stats)

    await backend.drain_queue("test_q")
    stats2 = await backend.queue_stats("test_q")
    assert stats2["message_count"] == 0


async def test_ack_does_not_force_completed_state(backend):
    payload = {"id": "ack1", "task_id": "noop", "args": [], "kwargs": {}}
    await backend.enqueue("test_q", payload)
    msg = await backend.dequeue("test_q")
    assert msg is not None

    await backend.update_job_state("test_q", "ack1", "delayed")
    await backend.ack("test_q", "ack1")

    state = await backend.get_job_state("test_q", "ack1")
    assert state == "delayed"


async def test_fetch_stalled_jobs_returns_queue_name_and_active_only(backend):
    old = time.time() - 30
    await backend._state.save(
        "test_q",
        "stalled-active",
        {
            "id": "stalled-active",
            "payload": {"id": "stalled-active", "task_id": "t", "args": [], "kwargs": {}},
            "status": "active",
            "heartbeat": old,
        },
    )
    await backend._state.save(
        "test_q",
        "stalled-waiting",
        {
            "id": "stalled-waiting",
            "payload": {"id": "stalled-waiting", "task_id": "t", "args": [], "kwargs": {}},
            "status": "waiting",
            "heartbeat": old,
        },
    )

    stalled = await backend.fetch_stalled_jobs(time.time() - 1)
    assert any(entry["queue_name"] == "test_q" and entry["job_data"]["id"] == "stalled-active" for entry in stalled)
    assert all(entry["job_data"]["id"] != "stalled-waiting" for entry in stalled)


async def test_update_and_get_job_state(backend, redis_store):
    payload = {"id": "j7", "task": "state_test"}
    await backend.enqueue("test_q", payload)
    await backend.update_job_state("test_q", "j7", "completed")
    state = await backend.get_job_state("test_q", "j7")
    assert state == "completed"


async def test_save_and_get_job_result(backend):
    payload = {"id": "j8", "task": "result_test"}
    await backend.enqueue("test_q", payload)
    await backend.save_job_result("test_q", "j8", {"answer": 42})
    result = await backend.get_job_result("test_q", "j8")
    assert result["answer"] == 42


async def test_complete_active_job_saves_result_and_acks_in_flight_message(backend, redis_store):
    payload = {"id": "life-complete", "task": "complete"}
    await backend.enqueue("test_q", payload)
    message = await backend.dequeue("test_q")

    assert message is not None
    assert ("test_q", "life-complete") in backend._in_flight

    await backend.save_heartbeat("test_q", "life-complete", time.time())
    await backend.complete_active_job("test_q", message["payload"], {"ok": True})

    entry = await redis_store.load("test_q", "life-complete")
    assert entry["status"] == "completed"
    assert entry["result"] == {"ok": True}
    assert "heartbeat" not in entry
    assert ("test_q", "life-complete") not in backend._in_flight


async def test_retry_active_job_saves_delayed_entry_and_acks_in_flight_message(backend, redis_store):
    payload = {"id": "life-retry", "task": "retry"}
    await backend.enqueue("test_q", payload)
    message = await backend.dequeue("test_q")

    assert message is not None
    assert ("test_q", "life-retry") in backend._in_flight

    await backend.save_heartbeat("test_q", "life-retry", time.time())
    run_at = time.time() + 60
    await backend.retry_active_job("test_q", {**message["payload"], "retries": 1}, run_at)

    entry = await redis_store.load("test_q", "life-retry")
    assert entry["status"] == "delayed"
    assert entry["run_at"] == pytest.approx(run_at)
    assert entry["payload"]["delay_until"] == pytest.approx(run_at)
    assert "heartbeat" not in entry
    assert ("test_q", "life-retry") not in backend._in_flight


async def test_fail_active_job_publishes_dlq_and_acks_in_flight_message(backend, redis_store):
    payload = {"id": "life-fail", "task": "fail"}
    await backend.enqueue("test_q", payload)
    message = await backend.dequeue("test_q")

    assert message is not None
    assert ("test_q", "life-fail") in backend._in_flight

    await backend.save_heartbeat("test_q", "life-fail", time.time())
    await backend.fail_active_job("test_q", {**message["payload"], "status": "failed"})

    entry = await redis_store.load("test_q", "life-fail")
    assert entry["status"] == "failed"
    assert "heartbeat" not in entry
    assert ("test_q", "life-fail") not in backend._in_flight

    dlq_job = await backend.dequeue("test_q.dlq")
    assert dlq_job is not None
    assert dlq_job["payload"]["id"] == "life-fail"
    await backend.ack("test_q.dlq", dlq_job["job_id"])


async def test_reenqueue_stalled_acks_in_flight_delivery(backend, redis_store):
    payload = {"id": "stalled-release", "task": "recover"}
    await backend.enqueue("test_q", payload)
    message = await backend.dequeue("test_q")

    assert message is not None
    assert ("test_q", "stalled-release") in backend._in_flight

    await backend.update_job_state("test_q", "stalled-release", "active")
    await backend.save_heartbeat("test_q", "stalled-release", time.time() - 10)

    stalled = await backend.fetch_stalled_jobs(time.time() - 1)
    entry = next(item for item in stalled if item["job_data"]["id"] == "stalled-release")
    await backend.reenqueue_stalled("test_q", entry["job_data"])

    assert ("test_q", "stalled-release") not in backend._in_flight
    state = await backend.get_job_state("test_q", "stalled-release")
    assert state == "waiting"

    recovered = await backend.dequeue("test_q")
    assert recovered is not None
    assert recovered["payload"]["id"] == "stalled-release"


async def test_reenqueue_stalled_after_restart_publishes_recovery_delivery(redis_store):
    queue = "test_q_stalled_restart"
    job_id = "stalled-restart"
    payload = {"id": job_id, "task": "recover-after-restart"}
    producer = RabbitMQBackend(rabbit_url=RABBIT_URL, job_store=redis_store, max_priority=None)
    recovery = RabbitMQBackend(rabbit_url=RABBIT_URL, job_store=redis_store, max_priority=None)

    await producer.drain_queue(queue)
    try:
        await producer.enqueue(queue, payload)
        message = await producer.dequeue(queue)
        assert message is not None
        assert (queue, job_id) in producer._in_flight

        await producer.update_job_state(queue, job_id, State.ACTIVE)
        old = time.time() - 10
        await producer.save_heartbeat(queue, job_id, old)

        assert queue in await recovery.list_queues()
        stalled = await recovery.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
        await recovery.reenqueue_stalled(queue, entry["job_data"])

        assert await recovery.get_job_state(queue, job_id) == State.WAITING
        recovered = await recovery.dequeue(queue)
        assert recovered is not None
        assert recovered["payload"]["id"] == job_id
        await recovery.complete_active_job(queue, recovered["payload"], {"ok": True})

        await producer.close()

        for _ in range(10):
            duplicate = await recovery.dequeue(queue)
            assert duplicate is None
            await asyncio.sleep(0.1)
    finally:
        await producer.close()
        await recovery.drain_queue(queue)
        await recovery.close()


async def test_stalled_recovery_releases_active_delivery_without_first_heartbeat(redis_store):
    queue = "test_q_stalled_without_first_heartbeat"
    job_id = "stalled-without-first-heartbeat"
    payload = {"id": job_id, "task": "recover-before-heartbeat"}
    producer = RabbitMQBackend(rabbit_url=RABBIT_URL, job_store=redis_store, max_priority=None)
    recovery = RabbitMQBackend(rabbit_url=RABBIT_URL, job_store=redis_store, max_priority=None)

    await producer.drain_queue(queue)
    try:
        await producer.enqueue(queue, payload)
        message = await producer.dequeue(queue)
        assert message is not None
        assert (queue, job_id) in producer._in_flight

        old = time.time() - 10
        entry = await redis_store.load(queue, job_id)
        entry.pop("heartbeat", None)
        entry["active_since"] = old
        entry["status"] = State.ACTIVE
        entry["payload"].pop("heartbeat", None)
        entry["payload"]["active_since"] = old
        entry["payload"]["status"] = State.ACTIVE
        await redis_store.save(queue, job_id, entry)

        stalled = await recovery.fetch_stalled_jobs(old + 1)
        entry = next(item for item in stalled if item["queue_name"] == queue and item["job_data"]["id"] == job_id)
        await recovery.reenqueue_stalled(queue, entry["job_data"])

        recovered = await recovery.dequeue(queue)
        assert recovered is not None
        assert recovered["payload"]["id"] == job_id
        await recovery.complete_active_job(queue, recovered["payload"], {"ok": True})
    finally:
        await producer.close()
        await recovery.drain_queue(queue)
        await recovery.close()


async def test_bulk_enqueue(backend):
    jobs = [
        {"id": "j9", "task": "bulk1"},
        {"id": "j10", "task": "bulk2"},
    ]
    await backend.bulk_enqueue("test_q", jobs)
    seen = set()
    for _ in jobs:
        msg = await backend.dequeue("test_q")
        assert msg is not None
        seen.add(msg["payload"]["id"])
    assert seen == {"j9", "j10"}


async def test_pause_resume(backend):
    # Pause an empty queue
    await backend.pause_queue("test_pause")
    assert await backend.is_queue_paused("test_pause") is True
    # Resume
    await backend.resume_queue("test_pause")
    assert await backend.is_queue_paused("test_pause") is False


async def test_pause_state_is_shared_between_backend_instances(backend, redis_store):
    queue = "test_pause_cross_instance"
    observer = RabbitMQBackend(rabbit_url=RABBIT_URL, job_store=redis_store, max_priority=None)
    try:
        await backend.pause_queue(queue)
        assert await observer.is_queue_paused(queue) is True

        await observer.resume_queue(queue)
        assert await backend.is_queue_paused(queue) is False
    finally:
        await observer.close()


@pytest.mark.parametrize("state", ["waiting", "delayed", "failed"])
async def test_list_jobs_by_state(backend, state):
    queue = "test_list_state"
    payload = {"id": "jx", "task": "filter_test"}
    if state == "waiting":
        await backend.enqueue(queue, payload)
    elif state == "delayed":
        await backend.enqueue_delayed(queue, payload, run_at=time.time() + 10)
    else:  # failed
        await backend.enqueue(queue, payload)
        await backend.move_to_dlq(queue, payload)

    jobs = await backend.list_jobs(queue, state)
    assert isinstance(jobs, list)
    # every returned job should carry our payload.task
    assert all(j["task"] == "filter_test" for j in jobs)


@pytest.mark.parametrize("state", ["waiting", "delayed", "failed"])
async def test_list_jobs_empty_queue(backend, state):
    jobs = await backend.list_jobs("no_such_queue", state)
    assert jobs == []


async def test_list_jobs_filters_correctly(backend):
    queue = "test_filters"
    j1 = {"id": "a", "task": "A"}
    j2 = {"id": "b", "task": "B"}
    j3 = {"id": "c", "task": "C"}

    await backend.enqueue(queue, j1)  # waiting
    await backend.enqueue_delayed(queue, j2, run_at=time.time() + 10)  # delayed
    await backend.enqueue(queue, j3)
    await backend.move_to_dlq(queue, j3)  # failed

    w = await backend.list_jobs(queue, "waiting")
    d = await backend.list_jobs(queue, "delayed")
    f = await backend.list_jobs(queue, "failed")

    assert all(j["task"] == "A" for j in w)
    assert all(j["task"] == "B" for j in d)
    assert all(j["task"] == "C" for j in f)


async def test_list_jobs_case_sensitive_state(backend):
    queue = "test_case"
    job = {"id": "xd", "task": "case"}
    await backend.enqueue(queue, job)
    # Mixed case should return empty
    jobs = await backend.list_jobs(queue, "Waiting")
    assert jobs == []


async def test_list_jobs_wrong_status_filter(backend):
    queue = "wrong_status"
    job = {"id": "xe", "task": "present"}
    await backend.enqueue(queue, job)
    jobs = await backend.list_jobs(queue, "nonexistent")
    assert jobs == []
