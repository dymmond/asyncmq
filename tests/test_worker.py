import anyio
import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.jobs import Job
from asyncmq.tasks import TASK_REGISTRY
from asyncmq.workers import Worker

pytestmark = pytest.mark.anyio


async def test_worker_heartbeat_registration_and_updates():
    backend = InMemoryBackend()
    settings.backend = backend
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


async def test_worker_processes_jobs_end_to_end():
    """
    The worker should pick up jobs from the queue, execute them, and mark them COMPLETED.
    We register a tiny async task into TASK_REGISTRY and enqueue it as N jobs.
    """
    backend = InMemoryBackend()
    settings.backend = backend

    # Tiny async task we register into the registry
    async def _echo_task(value: str) -> str:
        return value

    # Minimal registry entry used by handle_job()
    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._echo_task"] = {"func": _echo_task}

    # Create and enqueue jobs
    queue = "test_queue_end_to_end"
    j1 = Job(task_id="tests._echo_task", args=["a"], kwargs={}, job_id="j1")
    j2 = Job(task_id="tests._echo_task", args=["b"], kwargs={}, job_id="j2")

    await backend.enqueue(queue, j1.to_dict())
    await backend.enqueue(queue, j2.to_dict())

    # Start a worker
    worker = Worker(queue, heartbeat_interval=0.05)
    async with anyio.create_task_group() as tg:
        tg.start_soon(worker._run_with_scope)

        # Wait for both jobs to complete (bounded time)
        async def _wait_completed(job_id: str) -> None:
            with anyio.fail_after(2.0):
                while True:
                    state = await backend.get_job_state(queue, job_id)
                    if state == State.COMPLETED:
                        return
                    await anyio.sleep(0.02)

        await _wait_completed("j1")
        await _wait_completed("j2")

        # Stop the worker
        tg.cancel_scope.cancel()

    # Ensure queue is empty-ish and worker is deregistered
    workers = await backend.list_workers()
    assert workers == []


async def test_worker_respects_concurrency_limit():
    """
    Enqueue several slow jobs and assert that the peak concurrency observed
    never exceeds the worker.concurrency.
    """
    backend = InMemoryBackend()
    settings.backend = backend

    # Concurrency trackers
    max_seen = 0
    current = 0
    lock = anyio.Lock()

    async def _slow_task(delay: float = 0.1) -> str:
        nonlocal max_seen, current
        async with lock:
            current += 1
            if current > max_seen:
                max_seen = current
        try:
            await anyio.sleep(delay)
            return "ok"
        finally:
            async with lock:
                current -= 1

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._slow_task"] = {"func": _slow_task}

    queue = "test_queue_concurrency"
    jobs = [Job(task_id="tests._slow_task", args=[], kwargs={"delay": 0.15}, job_id=f"j{i}") for i in range(6)]
    for j in jobs:
        await backend.enqueue(queue, j.to_dict())

    # Worker with concurrency=2
    worker = Worker(queue, heartbeat_interval=0.05)
    worker.concurrency = 2

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker._run_with_scope)

        # Wait for all jobs to complete
        with anyio.fail_after(4.0):
            remaining = {j.id for j in jobs}
            while remaining:
                done = set()
                for job_id in list(remaining):
                    state = await backend.get_job_state(queue, job_id)
                    if state == State.COMPLETED:
                        done.add(job_id)
                remaining -= done
                if remaining:
                    await anyio.sleep(0.02)

        # Stop the worker
        tg.cancel_scope.cancel()

    # Peak concurrency must not exceed 2
    assert max_seen <= 2

    # Worker should be deregistered on shutdown
    workers = await backend.list_workers()
    assert workers == []


async def test_worker_lifecycle_hooks_invoked_in_order(monkeypatch):
    backend = InMemoryBackend()
    settings.backend = backend

    calls: list[str] = []

    async def s1(**kwargs):
        calls.append(f"s1:{kwargs['queue']}")

    def s2(**kwargs):  # sync hook also supported
        calls.append(f"s2:{kwargs['queue']}")

    async def sh1(**kwargs):
        calls.append(f"sh1:{kwargs['queue']}")

    def sh2(**kwargs):
        calls.append(f"sh2:{kwargs['queue']}")

    settings.worker_on_startup = [s1, s2]
    settings.worker_on_shutdown = (sh1, sh2)  # tuple is fine

    worker = Worker("q", heartbeat_interval=0.05)

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker._run_with_scope)
        # give startup time to fire + first heartbeat
        await anyio.sleep(0.05)
        # shutdown
        tg.cancel_scope.cancel()

    # ensure shutdown processed
    await anyio.sleep(0.01)

    # Expect startup hooks first, then shutdown hooks, preserving order
    assert calls[:2] == ["s1:q", "s2:q"]
    assert calls[2:] == ["sh1:q", "sh2:q"]


async def test_worker_startup_hook_failure_propagates(monkeypatch):
    backend = InMemoryBackend()
    settings.backend = backend

    async def bad(**kwargs):
        raise RuntimeError("boom")

    settings.worker_on_startup = [bad]
    settings.worker_on_shutdown = None

    worker = Worker("q", heartbeat_interval=0.05)

    with pytest.raises(RuntimeError, match="boom"):
        await worker._run_with_scope()


async def test_worker_shutdown_hook_failures_are_swallowed(monkeypatch):
    backend = InMemoryBackend()
    settings.backend = backend

    calls: list[str] = []

    async def ok_start(**kwargs):
        calls.append("start")

    async def bad_shutdown(**kwargs):
        calls.append("bad_shutdown")
        raise RuntimeError("boom-shutdown")

    settings.worker_on_startup = ok_start
    settings.worker_on_shutdown = [bad_shutdown]

    worker = Worker("q", heartbeat_interval=0.05)

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker._run_with_scope)
        await anyio.sleep(0.05)
        tg.cancel_scope.cancel()

    # no exception should escape both hooks called
    assert calls == ["start", "bad_shutdown"]
