import anyio
import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.conf import monkay
from asyncmq.workers import Worker

pytestmark = pytest.mark.anyio


async def test_worker_heartbeat_registration_and_updates():
    backend = InMemoryBackend()
    monkay.settings.backend = backend
    worker = Worker("test_queue", heartbeat_interval=0.1)

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker._run_with_scope)
        await anyio.sleep(0.05)

        # Check that worker is registered
        workers = await backend.list_workers()
        assert len(workers) == 1
        assert workers[0].id == worker.id
        assert workers[0].queue == "test_queue"
        assert workers[0].concurrency == worker.concurrency

        # Store initial heartbeat timestamp
        initial_heartbeat = workers[0].heartbeat

        # Wait for at least one heartbeat cycle
        await anyio.sleep(0.15)

        # Check that heartbeat was updated
        workers = await backend.list_workers()
        assert len(workers) == 1
        assert workers[0].heartbeat > initial_heartbeat

        # Cancel the worker
        tg.cancel_scope.cancel()

    # After cancellation, worker should be deregistered
    workers = await backend.list_workers()
    assert len(workers) == 0
