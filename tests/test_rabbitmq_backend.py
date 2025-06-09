import time

import pytest
import pytest_asyncio

from asyncmq.backends.rabbitmq import RabbitMQBackend
from asyncmq.stores.redis_store import RedisJobStore

pytestmark = pytest.mark.asyncio

RABBIT_URL = "amqp://guest:guest@localhost/"


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
    backend = RabbitMQBackend(rabbit_url=RABBIT_URL, job_store=redis_store)
    # Purge any pre-existing test queues
    await backend.drain_queue("test_q")
    await backend.drain_queue("test_q.dlq")
    yield backend
    # Cleanup after test
    await backend.drain_queue("test_q")
    await backend.drain_queue("test_q.dlq")
    await backend.close()


async def test_enqueue_and_dequeue_immediate(backend, redis_store):
    payload = {"id": "j1", "task": "do_something"}
    jid = await backend.enqueue("test_q", payload)
    assert jid == "j1"

    job = await backend.dequeue("test_q")
    assert job is not None
    assert job["payload"]["task"] == "do_something"

    await backend.ack("test_q", job["job_id"])
    state = await redis_store.load("test_q", "j1")
    assert state["status"] == "completed"


async def test_move_to_dlq(backend):
    payload = {"id": "j2", "task": "fail_me"}
    await backend.move_to_dlq("test_q", payload)
    dlq_job = await backend.dequeue("test_q.dlq")
    assert dlq_job is not None
    assert dlq_job["payload"]["task"] == "fail_me"


async def test_delayed_jobs(backend):
    now = time.time()
    payload = {"id": "j3", "task": "delayed"}
    await backend.enqueue_delayed("test_q", payload, run_at=now - 1)

    due = await backend.get_due_delayed("test_q")
    assert any(di.job_id == "j3" and di.payload["task"] == "delayed" for di in due)


async def test_list_and_remove_delayed(backend):
    run_at = time.time() + 10
    payload = {"id": "j4", "task": "later"}
    await backend.enqueue_delayed("test_q", payload, run_at=run_at)
    lst = await backend.list_delayed("test_q")
    assert any(di.job_id == "j4" for di in lst)

    await backend.remove_delayed("test_q", "j4")
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

    await backend.drain_queue("test_q")
    stats2 = await backend.queue_stats("test_q")
    assert stats2["message_count"] == 0


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
    assert all(j["payload"]["task"] == "filter_test" for j in jobs)


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

    assert all(j["payload"]["task"] == "A" for j in w)
    assert all(j["payload"]["task"] == "B" for j in d)
    assert all(j["payload"]["task"] == "C" for j in f)


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
