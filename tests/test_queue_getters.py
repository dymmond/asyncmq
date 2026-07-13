import time
from uuid import uuid4

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.enums import State
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend
from asyncmq.jobs import Job
from asyncmq.queues import Queue
from asyncmq.tasks import TASK_REGISTRY, task

pytestmark = pytest.mark.anyio


@task(queue="inspect")
async def inspect_task(value=None):
    return value


def get_task_id(func):
    for task_id, meta in TASK_REGISTRY.items():
        if meta["func"] == func:
            return task_id
    raise ValueError(f"Task {func.__name__} not registered.")


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


async def build_inspection_queue(backend) -> tuple[Queue, str]:
    queue_name = f"inspect-{uuid4().hex}"
    return Queue(queue_name, backend=backend), get_task_id(inspect_task)


async def seed_jobs(queue: Queue, task_id: str) -> dict[str, Job]:
    waiting = Job(task_id=task_id, args=["waiting"], kwargs={})
    delayed = Job(task_id=task_id, args=["delayed"], kwargs={})
    completed = Job(task_id=task_id, args=["completed"], kwargs={})
    failed = Job(task_id=task_id, args=["failed"], kwargs={})
    parent = Job(task_id=task_id, args=["parent"], kwargs={})
    child = Job(task_id=task_id, args=["child"], kwargs={}, depends_on=[parent.id])

    await queue.backend.enqueue(queue.name, waiting.to_dict())
    await queue.backend.enqueue_delayed(queue.name, delayed.to_dict(), time.time() + 60)

    await queue.backend.enqueue(queue.name, completed.to_dict())
    await queue.backend.update_job_state(queue.name, completed.id, State.COMPLETED)
    await queue.backend.save_job_result(queue.name, completed.id, "done")

    await queue.backend.move_to_dlq(queue.name, {**failed.to_dict(), "status": State.FAILED})

    await queue.backend.enqueue(queue.name, child.to_dict())
    await queue.backend.add_dependencies(queue.name, child.to_dict())

    return {
        "waiting": waiting,
        "delayed": delayed,
        "completed": completed,
        "failed": failed,
        "parent": parent,
        "child": child,
    }


async def test_queue_getters_report_counts_and_dependency_state(backend):
    queue, task_id = await build_inspection_queue(backend)
    jobs = await seed_jobs(queue, task_id)

    child = await queue.get_job(jobs["child"].id)
    completed = await queue.get_job(jobs["completed"].id)

    assert child is not None
    assert completed is not None
    assert child["depends_on"] == [jobs["parent"].id]
    assert completed["result"] == "done"
    assert await queue.get_job_state(jobs["child"].id) == "waiting-children"
    assert await queue.get_job_result(jobs["completed"].id) == "done"

    counts = await queue.get_job_counts(
        "waiting",
        "delayed",
        "completed",
        "failed",
        "waiting-children",
        "paused",
        "prioritized",
    )

    assert counts == {
        "waiting": 1,
        "delayed": 1,
        "completed": 1,
        "failed": 1,
        "waiting-children": 1,
        "paused": 0,
        "prioritized": 0,
    }
    assert await queue.get_job_count_by_types("waiting", "delayed", "waiting-children") == 3
    assert await queue.count() == 3


async def test_queue_get_jobs_supports_pagination(backend):
    queue, task_id = await build_inspection_queue(backend)
    jobs = await seed_jobs(queue, task_id)

    page = await queue.get_jobs(["waiting", "delayed", "waiting-children"], start=0, end=1, asc=True)
    waiting_children = await queue.get_waiting_children(asc=True)

    assert [job["id"] for job in page] == [jobs["waiting"].id, jobs["delayed"].id]
    assert [job["id"] for job in waiting_children] == [jobs["child"].id]


async def test_queue_drain_and_obliterate_follow_admin_semantics(backend):
    queue, task_id = await build_inspection_queue(backend)

    waiting = Job(task_id=task_id, args=["waiting"], kwargs={})
    delayed = Job(task_id=task_id, args=["delayed"], kwargs={})
    child = Job(task_id=task_id, args=["child"], kwargs={}, depends_on=["missing-parent"])
    active = Job(task_id=task_id, args=["active"], kwargs={})

    await queue.backend.enqueue(queue.name, waiting.to_dict())
    await queue.backend.enqueue_delayed(queue.name, delayed.to_dict(), time.time() + 60)
    await queue.backend.enqueue(queue.name, child.to_dict())
    await queue.backend.add_dependencies(queue.name, child.to_dict())
    await queue.backend.enqueue(queue.name, active.to_dict())
    await queue.backend.update_job_state(queue.name, active.id, State.ACTIVE)

    drained = await queue.drain()
    assert waiting.id in drained
    assert child.id not in drained
    assert await queue.get_job(waiting.id) is None
    assert await queue.get_job(delayed.id) is not None
    assert await queue.get_job(child.id) is not None

    drained_with_delayed = await queue.drain(include_delayed=True)
    assert delayed.id in drained_with_delayed
    assert await queue.get_job(delayed.id) is None

    with pytest.raises(RuntimeError):
        await queue.obliterate()

    removed = await queue.obliterate(force=True)
    assert active.id in removed
    assert child.id in removed
    assert await queue.get_job(active.id) is None
    assert await queue.get_job(child.id) is None


async def test_queue_clean_removes_waiting_backend_membership(backend):
    queue, task_id = await build_inspection_queue(backend)
    waiting = Job(task_id=task_id, args=["waiting"], kwargs={})

    await queue.backend.enqueue(queue.name, waiting.to_dict())

    await queue.clean(State.WAITING)

    assert await queue.get_job(waiting.id) is None
    assert await queue.backend.dequeue(queue.name) is None


async def test_queue_remove_and_retry_wrap_backend_admin_apis(backend):
    queue, task_id = await build_inspection_queue(backend)

    waiting = Job(task_id=task_id, args=["waiting"], kwargs={})
    failed = Job(task_id=task_id, args=["failed"], kwargs={})

    await queue.backend.enqueue(queue.name, waiting.to_dict())
    await queue.backend.move_to_dlq(queue.name, {**failed.to_dict(), "status": State.FAILED})

    assert await queue.remove_job(waiting.id) is True
    assert await queue.get_job(waiting.id) is None

    assert await queue.retry_job(failed.id) is True
    retried = await queue.get_job(failed.id)
    assert retried is not None
    assert retried["status"] == State.WAITING
