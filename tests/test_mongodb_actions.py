import time

import pytest

from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.core.enums import State


@pytest.mark.anyio
async def test_cancel_job_mongo():
    backend = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database="asyncmq_test")
    await backend.connect()

    queue = "q1"
    job = {"id": "m1"}
    backend.queues.setdefault(queue, []).append(job.copy())
    backend.delayed.setdefault(queue, []).append((time.time(), job.copy()))

    await backend.cancel_job(queue, "m1")

    assert all(j["id"] != "m1" for j in backend.queues.get(queue, []))
    assert all(j["id"] != "m1" for _, j in backend.delayed.get(queue, []))
    assert await backend.is_job_cancelled(queue, "m1")


@pytest.mark.anyio
async def test_retry_job_mongo():
    backend = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database="asyncmq_test")
    await backend.connect()

    queue = "q1"
    job_id = "m2"
    await backend.store.save(queue, job_id, {"id": job_id, "status": State.FAILED})

    await backend.retry_job(queue, job_id)

    job = await backend.store.load(queue, job_id)
    assert job["status"] == State.WAITING


@pytest.mark.anyio
async def test_remove_job_mongo():
    backend = MongoDBBackend(mongo_url="mongodb://root:mongoadmin@localhost:27017", database="asyncmq_test")
    await backend.connect()

    queue = "q1"
    job_id = "m3"
    await backend.store.save(queue, job_id, {"id": job_id, "status": State.WAITING})

    await backend.remove_job(queue, job_id)

    job = await backend.store.load(queue, job_id)
    assert job is None
