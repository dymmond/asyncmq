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
        rabbit_url="amqp://guest:guest@localhost:5672/", prefetch_count=1, redis_url="redis://localhost:6379"
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

    # Dequeue in order
    deq1 = await redis_backend.dequeue("rq")
    deq2 = await redis_backend.dequeue("rq")
    assert deq1["id"] == "id1"
    assert deq2["id"] == "id2"

    # Check Redis HSET for dependencies
    deps = await redis_backend.redis.hkeys("queue:rq:deps:id1")
    # hkeys may return bytes or strings; normalize to strings
    deps = [d.decode() if isinstance(d, bytes) else d for d in deps]
    assert "id2" in deps


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


async def test_rabbitmq_atomic_add_flow(rabbitmq_backend):
    fp = FlowProducer(backend=rabbitmq_backend)
    job1 = Job(task_id="t5", args=[], kwargs={}, job_id="id1")
    job2 = Job(task_id="t5", args=[], kwargs={}, job_id="id2", depends_on=["id1"])

    ids = await fp.add_flow("rrq", [job1, job2])
    assert ids == ["id1", "id2"]

    # Dequeue in order
    first = await rabbitmq_backend.dequeue("rrq")
    assert first and first["payload"]["id"] == "id1"

    second = await rabbitmq_backend.dequeue("rrq")
    assert second and second["payload"]["id"] == "id2"
