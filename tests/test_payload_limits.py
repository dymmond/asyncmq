import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.conf import settings
from asyncmq.core.payload import job_payload_size_bytes, validate_job_payload_size
from asyncmq.exceptions import PayloadTooLarge
from asyncmq.queues import Queue
from asyncmq.tasks import task

pytestmark = pytest.mark.anyio


def test_validate_job_payload_size_allows_payload_under_limit():
    payload = {"id": "j1", "task": "t", "args": [], "kwargs": {}}

    validate_job_payload_size(payload, max_bytes=1_024, dumps=settings.json_serializer.to_json)


def test_validate_job_payload_size_rejects_payload_over_limit():
    payload = {"id": "j1", "task": "t", "args": ["x" * 128], "kwargs": {}}
    size = job_payload_size_bytes(payload, settings.json_serializer.to_json)

    with pytest.raises(PayloadTooLarge, match=str(size)):
        validate_job_payload_size(payload, max_bytes=size - 1, dumps=settings.json_serializer.to_json)


async def test_queue_add_rejects_oversized_payload_before_enqueue():
    backend = InMemoryBackend()
    queue = Queue("payload-limit", backend=backend)
    previous_limit = settings.max_job_payload_bytes
    settings.max_job_payload_bytes = 128
    try:
        with pytest.raises(PayloadTooLarge):
            await queue.add("tasks.large", args=["x" * 512])
    finally:
        settings.max_job_payload_bytes = previous_limit

    assert await backend.dequeue("payload-limit") is None


async def test_queue_enqueue_rejects_oversized_custom_payload():
    backend = InMemoryBackend()
    queue = Queue("payload-limit-direct", backend=backend)
    previous_limit = settings.max_job_payload_bytes
    settings.max_job_payload_bytes = 128
    try:
        with pytest.raises(PayloadTooLarge):
            await queue.enqueue({"id": "j1", "task": "tasks.large", "args": ["x" * 512], "kwargs": {}})
    finally:
        settings.max_job_payload_bytes = previous_limit

    assert await backend.dequeue("payload-limit-direct") is None


async def test_task_enqueue_rejects_oversized_payload_before_backend_enqueue():
    backend = InMemoryBackend()
    previous_limit = settings.max_job_payload_bytes
    settings.max_job_payload_bytes = 128

    @task(queue="payload-limit-task")
    async def large_task(value: str) -> str:
        return value

    try:
        with pytest.raises(PayloadTooLarge):
            await large_task.enqueue("x" * 512, backend=backend)
    finally:
        settings.max_job_payload_bytes = previous_limit

    assert await backend.dequeue("payload-limit-task") is None


async def test_payload_limit_can_be_disabled():
    backend = InMemoryBackend()
    queue = Queue("payload-limit-disabled", backend=backend)
    previous_limit = settings.max_job_payload_bytes
    settings.max_job_payload_bytes = None
    try:
        job_id = await queue.add("tasks.large", args=["x" * 512])
    finally:
        settings.max_job_payload_bytes = previous_limit

    assert job_id is not None
    assert await backend.dequeue("payload-limit-disabled") is not None
