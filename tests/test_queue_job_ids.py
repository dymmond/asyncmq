from uuid import uuid4

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend
from asyncmq.queues import Queue
from asyncmq.tasks import TASK_REGISTRY, task

pytestmark = pytest.mark.anyio


@task(queue="job-ids")
async def job_id_task(value=None):
    return value


def get_task_id() -> str:
    for task_id in TASK_REGISTRY:
        if task_id.endswith("job_id_task"):
            return task_id
    raise AssertionError("job_id_task not registered")


@pytest.fixture(params=["memory", "redis", "mongodb", "postgres"])
async def backend(request):
    name = request.param
    if name == "memory":
        yield InMemoryBackend()
        return

    if name == "redis":
        backend = RedisBackend()
        await backend.redis.flushall()
        try:
            yield backend
        finally:
            await backend.redis.flushall()
            await backend.redis.aclose()
            await backend.job_store.redis.aclose()
        return

    if name == "mongodb":
        db_name = f"test_asyncmq_{uuid4().hex}"
        backend = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database=db_name)
        yield backend
        await backend.store.client.drop_database(db_name)
        return

    if name == "postgres":
        await install_or_drop_postgres_backend(drop=True)
        await install_or_drop_postgres_backend(drop=False)
        backend = PostgresBackend()
        yield backend
        await install_or_drop_postgres_backend(drop=True)
        return

    pytest.skip(f"Unsupported backend: {name}")


async def test_queue_add_skips_duplicate_custom_job_ids(backend):
    queue = Queue(f"job-ids-{uuid4().hex}", backend=backend)
    task_id = get_task_id()

    first = await queue.add(task_id, args=["first"], job_id="job-alpha")
    second = await queue.add(task_id, args=["second"], job_id="job-alpha")

    jobs = await queue.get_waiting(asc=True)

    assert first == "job-alpha"
    assert second == "job-alpha"
    assert [job["id"] for job in jobs] == ["job-alpha"]
    assert jobs[0]["args"] == ["first"]


async def test_queue_add_bulk_accepts_job_ids_and_skips_duplicates(backend):
    queue = Queue(f"job-ids-{uuid4().hex}", backend=backend)
    task_id = get_task_id()

    existing = await queue.add(task_id, args=["existing"], job_id="job-existing")
    ids = await queue.add_bulk(
        [
            {"task_id": task_id, "args": ["fresh"], "job_id": "job-fresh"},
            {"task_id": task_id, "args": ["duplicate-existing"], "job_id": "job-existing"},
            {"task_id": task_id, "args": ["duplicate-batch"], "jobId": "job-fresh"},
            {"task_id": task_id, "args": ["auto"]},
        ]
    )

    waiting = await queue.get_waiting(asc=True)
    waiting_by_id = {job["id"]: job for job in waiting}

    assert ids[0] == "job-fresh"
    assert ids[1] == existing
    assert ids[2] == "job-fresh"
    assert len(ids[3]) > 0

    assert set(waiting_by_id) == {existing, "job-fresh", ids[3]}
    assert waiting_by_id[existing]["args"] == ["existing"]
    assert waiting_by_id["job-fresh"]["args"] == ["fresh"]
    assert waiting_by_id[ids[3]]["args"] == ["auto"]


async def test_queue_add_rejects_invalid_custom_job_ids():
    queue = Queue("job-id-validation", backend=InMemoryBackend())
    task_id = get_task_id()

    with pytest.raises(ValueError, match="numeric-only"):
        await queue.add(task_id, job_id="123")

    with pytest.raises(ValueError, match="contain ':'"):
        await queue.add(task_id, job_id="bad:id")
