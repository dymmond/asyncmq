import time

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.logging import logger
from asyncmq.queues import Queue

pytestmark = pytest.mark.anyio


async def test_add_repeatable_every_only():
    queue = Queue(name="test-every")

    queue.add_repeatable(
        task_id="cleanup_temp",
        every=300,
        args=[1, 2, 3],
        kwargs={"foo": "bar"},
        retries=2,
        ttl=600,
        priority=1,
    )

    logger.debug(f"Queue repeatables: {queue._repeatables}")
    assert len(queue._repeatables) == 1
    job = queue._repeatables[0]
    assert job["task_id"] == "cleanup_temp"
    assert job["every"] == 300
    assert "cron" not in job


async def test_add_repeatable_cron_only():
    queue = Queue(name="test-cron")

    queue.add_repeatable(
        task_id="send_summary",
        cron="*/5 * * * *",
    )

    assert len(queue._repeatables) == 1
    job = queue._repeatables[0]
    assert job["task_id"] == "send_summary"
    assert job["cron"] == "*/5 * * * *"
    assert "every" not in job


async def test_add_repeatable_missing_parameters():
    queue = Queue(name="test-missing")

    with pytest.raises(ValueError):
        queue.add_repeatable(
            task_id="do_nothing",
            args=[],
            kwargs={},
        )


async def test_repeatables_mixed_types():
    queue = Queue(name="test-mixed")

    queue.add_repeatable(task_id="cron_task", cron="*/1 * * * *")
    queue.add_repeatable(task_id="interval_task", every=60)

    assert len(queue._repeatables) == 2

    cron_job = next(job for job in queue._repeatables if "cron" in job)
    interval_job = next(job for job in queue._repeatables if "every" in job)

    assert cron_job["task_id"] == "cron_task"
    assert interval_job["task_id"] == "interval_task"


async def test_upsert_repeatable_persists_backend_definition():
    backend = InMemoryBackend()
    queue = Queue(name="test-durable", backend=backend)

    next_run = await queue.upsert_repeatable(
        task_id="cleanup_temp",
        every=60,
        args=[1],
        kwargs={"tenant": "acme"},
        retries=2,
        ttl=600,
        priority=2,
        backoff=3,
    )

    assert next_run >= time.time()

    repeatables = await queue.list_repeatables()
    assert len(repeatables) == 1
    record = repeatables[0]
    assert record.job_def["task_id"] == "cleanup_temp"
    assert record.job_def["every"] == 60
    assert record.job_def["retries"] == 2
    assert record.job_def["backoff"] == 3
    assert record.paused is False


async def test_remove_repeatable_uses_backend_registry():
    backend = InMemoryBackend()
    queue = Queue(name="test-durable-remove", backend=backend)

    await queue.upsert_repeatable(task_id="cleanup_temp", every=60)
    repeatables = await queue.list_repeatables()
    assert len(repeatables) == 1

    await queue.remove_repeatable(repeatables[0].job_def)

    remaining = await queue.list_repeatables()
    assert remaining == []
