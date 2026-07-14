from uuid import uuid4

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.rabbitmq import RabbitMQBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.enums import State
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job

pytestmark = pytest.mark.anyio


# Fixtures for each backend
@pytest.fixture
async def memory_backend():
    return InMemoryBackend()


@pytest.fixture
async def redis_backend():
    backend = RedisBackend(redis_url_or_client="redis://localhost:6379")
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


@pytest.fixture
async def rabbitmq_backend(redis_backend):
    """
    RabbitMQBackend powered by a RedisJobStore for metadata.
    """
    # 1) flush the raw Redis DB
    await redis_backend.redis.flushdb()

    # 3) instantiate RabbitMQBackend with the job_store (not the RedisBackend!)
    backend = RabbitMQBackend(
        rabbit_url="amqp://guest:guest@localhost:5672/",
        prefetch_count=1,
        redis_url="redis://localhost:6379",
        max_priority=None,
    )

    yield backend

    # teardown: close RabbitMQ and flush Redis again
    await backend.close()
    await redis_backend.redis.flushdb()


async def test_redis_atomic_add_flow(redis_backend):
    fp = FlowProducer(backend=redis_backend)
    job1 = Job(task_id="t2", args=[], kwargs={}, job_id="id1")
    job2 = Job(task_id="t2", args=[], kwargs={}, job_id="id2", depends_on=["id1"])

    ids = await fp.add_flow("rq", [job1, job2])
    assert ids == ["id1", "id2"]

    waiting_children = await redis_backend.list_jobs("rq", "waiting-children")
    assert [job["id"] for job in waiting_children] == ["id2"]

    # Children must not be dequeueable until the parent resolves.
    deq1 = await redis_backend.dequeue("rq")
    deq2 = await redis_backend.dequeue("rq")
    assert deq1["id"] == "id1"
    assert deq1["has_dependents"] is True
    assert deq2 is None

    # Check Redis dependency indexes used by resolve_dependency.
    deps = await redis_backend.redis.smembers("deps:rq:parent:id1")
    # hkeys may return bytes or strings; normalize to strings
    deps = [d.decode() if isinstance(d, bytes) else d for d in deps]
    assert "id2" in deps

    await redis_backend.complete_active_job("rq", {**deq1, "status": State.ACTIVE}, "ok")
    await redis_backend.resolve_dependency("rq", "id1")

    deq2 = await redis_backend.dequeue("rq")
    assert deq2 and deq2["id"] == "id2"
    assert deq2["has_dependents"] is False
    assert "depends_on" not in deq2


async def test_postgres_atomic_add_flow(postgres_backend):
    backend = postgres_backend
    fp = FlowProducer(backend=backend)
    job1 = Job(task_id="t3", args=[], kwargs={}, job_id="id1")
    job2 = Job(task_id="t3", args=[], kwargs={}, job_id="id2", depends_on=["id1"])

    ids = await fp.add_flow("pq", [job1, job2])
    assert ids == ["id1", "id2"]

    # Verify stored jobs
    data1 = await backend.store.load("pq", "id1")
    data2 = await backend.store.load("pq", "id2")
    assert data1["status"] == State.WAITING
    assert data2["status"] == State.WAITING
    assert data2.get("depends_on") == ["id1"]

    waiting = await backend.list_jobs("pq", "waiting")
    waiting_children = await backend.list_jobs("pq", "waiting-children")
    assert [job["id"] for job in waiting] == ["id1"]
    assert [job["id"] for job in waiting_children] == ["id2"]

    deq1 = await backend.dequeue("pq")
    deq2 = await backend.dequeue("pq")
    assert deq1 and deq1["id"] == "id1"
    assert deq2 is None

    await backend.complete_active_job("pq", {**deq1, "status": State.ACTIVE}, "ok")
    await backend.resolve_dependency("pq", "id1")
    deq2 = await backend.dequeue("pq")
    assert deq2 and deq2["id"] == "id2"
    assert "depends_on" not in deq2


async def test_mongodb_atomic_add_flow(mongodb_backend):
    backend = mongodb_backend
    fp = FlowProducer(backend=backend)
    job1 = Job(task_id="t4", args=[], kwargs={}, job_id="id1")
    job2 = Job(task_id="t4", args=[], kwargs={}, job_id="id2", depends_on=["id1"])

    ids = await fp.add_flow("mq", [job1, job2])
    assert ids == ["id1", "id2"]

    # Fetch from MongoDB store
    doc1 = await backend.store.load("mq", "id1")
    doc2 = await backend.store.load("mq", "id2")
    assert doc1["status"] == State.WAITING
    assert doc2["status"] == State.WAITING
    assert doc2.get("depends_on") == ["id1"]

    waiting = await backend.list_jobs("mq", "waiting")
    waiting_children = await backend.list_jobs("mq", "waiting-children")
    assert [job["id"] for job in waiting] == ["id1"]
    assert [job["id"] for job in waiting_children] == ["id2"]

    deq1 = await backend.dequeue("mq")
    deq2 = await backend.dequeue("mq")
    assert deq1 and deq1["id"] == "id1"
    assert deq2 is None

    await backend.complete_active_job("mq", {**deq1, "status": State.ACTIVE}, "ok")
    await backend.resolve_dependency("mq", "id1")
    deq2 = await backend.dequeue("mq")
    assert deq2 and deq2["id"] == "id2"
    assert "depends_on" not in deq2


async def test_rabbitmq_atomic_add_flow(rabbitmq_backend):
    queue_name = f"rrq-{uuid4().hex}"
    try:
        fp = FlowProducer(backend=rabbitmq_backend)
        job1 = Job(task_id="t5", args=[], kwargs={}, job_id="id1")
        job2 = Job(task_id="t5", args=[], kwargs={}, job_id="id2", depends_on=["id1"])

        ids = await fp.add_flow(queue_name, [job1, job2])
        assert ids == ["id1", "id2"]

        waiting_children = await rabbitmq_backend.list_jobs(queue_name, "waiting-children")
        assert [job["id"] for job in waiting_children] == ["id2"]

        # Children must not be published until the parent resolves.
        first = await rabbitmq_backend.dequeue(queue_name)
        assert first and first["payload"]["id"] == "id1"

        second = await rabbitmq_backend.dequeue(queue_name)
        assert second is None

        await rabbitmq_backend.complete_active_job(queue_name, {**first["payload"], "status": State.ACTIVE}, "ok")
        await rabbitmq_backend.resolve_dependency(queue_name, "id1")
        second = await rabbitmq_backend.dequeue(queue_name)
        assert second and second["payload"]["id"] == "id2"
        assert "depends_on" not in second["payload"]
        await rabbitmq_backend.ack(queue_name, second["job_id"])
    finally:
        await rabbitmq_backend._connect()
        await rabbitmq_backend._chan.queue_delete(queue_name, if_unused=False, if_empty=False)


async def test_inmemory_flow_blocks_child_until_dependencies_resolve(memory_backend):
    fp = FlowProducer(backend=memory_backend)
    job1 = Job(task_id="t1", args=[], kwargs={}, job_id="id1")
    job2 = Job(task_id="t2", args=[], kwargs={}, job_id="id2")
    child = Job(task_id="t3", args=[], kwargs={}, job_id="id3", depends_on=["id1", "id2"])

    ids = await fp.add_flow("memq", [job1, job2, child])
    assert ids == ["id1", "id2", "id3"]

    first = await memory_backend.dequeue("memq")
    second = await memory_backend.dequeue("memq")
    third = await memory_backend.dequeue("memq")
    assert first and first["id"] == "id1"
    assert second and second["id"] == "id2"
    assert third is None

    await memory_backend.complete_active_job("memq", {**first, "status": State.ACTIVE}, "ok")
    await memory_backend.resolve_dependency("memq", "id1")
    assert await memory_backend.dequeue("memq") is None

    await memory_backend.complete_active_job("memq", {**second, "status": State.ACTIVE}, "ok")
    await memory_backend.resolve_dependency("memq", "id2")
    ready = await memory_backend.dequeue("memq")
    assert ready and ready["id"] == "id3"
    assert "depends_on" not in ready
