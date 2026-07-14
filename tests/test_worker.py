from types import SimpleNamespace
from typing import Any

import anyio
import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.jobs import Job
from asyncmq.tasks import TASK_REGISTRY
from asyncmq.workers import (
    Worker,
    _next_worker_idle_sleep,
    _worker_idle_poll_interval,
    handle_job,
    process_job,
)

pytestmark = pytest.mark.anyio


def test_worker_idle_backoff_is_bounded_and_configurable():
    worker_settings = SimpleNamespace(worker_idle_poll_interval=0.05, worker_idle_poll_max_interval=0.2)

    assert _worker_idle_poll_interval(worker_settings) == 0.05
    assert _next_worker_idle_sleep(0.05, worker_settings) == 0.1
    assert _next_worker_idle_sleep(0.1, worker_settings) == 0.2
    assert _next_worker_idle_sleep(0.2, worker_settings) == 0.2


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


async def test_worker_heartbeat_renewal_failure_does_not_stop_worker():
    class FlakyWorkerHeartbeatBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.register_calls = 0

        async def register_worker(
            self,
            worker_id: str,
            queue: str,
            concurrency: int,
            timestamp: float | None = None,
        ) -> None:
            self.register_calls += 1
            if self.register_calls == 2:
                raise RuntimeError("temporary worker heartbeat outage")
            await super().register_worker(worker_id, queue, concurrency, timestamp)

    backend = FlakyWorkerHeartbeatBackend()
    settings.backend = backend
    worker = Worker("test_queue_flaky_worker_heartbeat", heartbeat_interval=0.05)

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker._run_with_scope)
        with anyio.fail_after(1):
            while backend.register_calls < 3:
                await anyio.sleep(0.01)

        workers = await backend.list_workers()
        assert len(workers) == 1
        assert workers[0].id == worker.id
        tg.cancel_scope.cancel()

    assert await backend.list_workers() == []


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


async def test_started_event_listener_failure_does_not_fail_job():
    backend = InMemoryBackend()
    queue = "test_queue_event_listener_failure"
    ran: list[str] = []

    async def _task() -> str:
        ran.append("handler")
        return "ok"

    def _bad_listener(data: dict[str, Any]) -> None:
        raise RuntimeError(f"listener failed for {data['id']}")

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._event_listener_task"] = {"func": _task}
    event_emitter._listeners.clear()
    event_emitter.on("job:started", _bad_listener)

    try:
        job = Job(
            task_id="tests._event_listener_task",
            args=[],
            kwargs={},
            job_id="event-listener-failure",
            max_retries=0,
        )
        await backend.enqueue(queue, job.to_dict())
        raw = await backend.dequeue(queue)
        assert raw is not None

        await handle_job(queue, raw, backend)

        assert ran == ["handler"]
        assert await backend.get_job_state(queue, job.id) == State.COMPLETED
        assert await backend.get_job_result(queue, job.id) == "ok"
    finally:
        event_emitter._listeners.clear()


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


async def test_worker_does_not_prefetch_beyond_concurrency():
    """
    A worker must not dequeue more jobs than it can actively execute.
    """
    backend = InMemoryBackend()
    settings.backend = backend

    started = 0

    async def _slow_task(delay: float = 0.3) -> str:
        nonlocal started
        started += 1
        await anyio.sleep(delay)
        return "ok"

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._prefetch_task"] = {"func": _slow_task}

    queue = "test_queue_prefetch"
    jobs = [Job(task_id="tests._prefetch_task", args=[], kwargs={}, job_id=f"j{i}") for i in range(5)]
    for job in jobs:
        await backend.enqueue(queue, job.to_dict())

    async with anyio.create_task_group() as tg:
        tg.start_soon(process_job, queue, anyio.CapacityLimiter(1), None, backend)
        await anyio.sleep(0.05)

        active = sum(1 for queued_name, _ in backend.active_jobs if queued_name == queue)
        waiting = len(backend.queues.get(queue, []))

        tg.cancel_scope.cancel()

    assert started == 1
    assert active <= 1
    assert waiting >= 4


async def test_process_job_drain_stops_claiming_new_jobs_after_inflight_finishes():
    backend = InMemoryBackend()
    settings.backend = backend
    started = anyio.Event()
    release = anyio.Event()
    drain_event = anyio.Event()

    async def _drained_task() -> str:
        started.set()
        await release.wait()
        return "done"

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._drained_task"] = {"func": _drained_task}

    queue = "test_queue_process_drain"
    first = Job(task_id="tests._drained_task", args=[], kwargs={}, job_id="j1")
    second = Job(task_id="tests._drained_task", args=[], kwargs={}, job_id="j2")
    await backend.enqueue(queue, first.to_dict())
    await backend.enqueue(queue, second.to_dict())

    async def run_processor() -> None:
        await process_job(
            queue,
            anyio.CapacityLimiter(1),
            None,
            backend,
            drain_event=drain_event,
        )

    with anyio.fail_after(2.0):
        async with anyio.create_task_group() as tg:
            tg.start_soon(run_processor)
            await started.wait()
            drain_event.set()
            await anyio.sleep(0.05)

            assert await backend.get_job_state(queue, "j2") == State.WAITING

            release.set()

    assert await backend.get_job_state(queue, "j1") == State.COMPLETED
    assert await backend.get_job_state(queue, "j2") == State.WAITING


async def test_worker_drain_deregisters_after_inflight_job_finishes():
    backend = InMemoryBackend()
    settings.backend = backend
    started = anyio.Event()
    release = anyio.Event()
    drained = anyio.Event()

    async def _drained_task() -> str:
        started.set()
        await release.wait()
        return "done"

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._worker_drained_task"] = {"func": _drained_task}

    queue = "test_queue_worker_drain"
    first = Job(task_id="tests._worker_drained_task", args=[], kwargs={}, job_id="j1")
    second = Job(task_id="tests._worker_drained_task", args=[], kwargs={}, job_id="j2")
    await backend.enqueue(queue, first.to_dict())
    await backend.enqueue(queue, second.to_dict())

    worker = Worker(queue, heartbeat_interval=0.05)
    worker.concurrency = 1

    async def drain_worker() -> None:
        await worker.drain()
        drained.set()

    with anyio.fail_after(2.0):
        async with anyio.create_task_group() as tg:
            tg.start_soon(worker._run_with_scope)
            await started.wait()
            tg.start_soon(drain_worker)
            await anyio.sleep(0.05)

            assert not drained.is_set()
            assert await backend.get_job_state(queue, "j2") == State.WAITING

            release.set()
            await drained.wait()

    assert await backend.get_job_state(queue, "j1") == State.COMPLETED
    assert await backend.get_job_state(queue, "j2") == State.WAITING
    assert await backend.list_workers() == []


async def test_worker_success_uses_backend_lifecycle_transition():
    class LifecycleBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.completed_calls = 0

        async def complete_active_job(self, queue_name: str, payload: dict[str, Any], result: Any) -> None:
            self.completed_calls += 1
            await super().complete_active_job(queue_name, payload, result)

        async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
            raise AssertionError("worker must complete through complete_active_job")

        async def ack(self, queue_name: str, job_id: str) -> None:
            raise AssertionError("worker must complete through complete_active_job")

    backend = LifecycleBackend()
    settings.backend = backend

    async def _task() -> str:
        return "done"

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._lifecycle_success"] = {"func": _task}

    queue = "test_queue_lifecycle_success"
    job = Job(task_id="tests._lifecycle_success", args=[], kwargs={}, job_id="j-success")
    await backend.enqueue(queue, job.to_dict())
    payload = await backend.dequeue(queue)

    assert payload is not None

    await handle_job(queue, payload, backend)

    assert backend.completed_calls == 1
    assert await backend.get_job_state(queue, "j-success") == State.COMPLETED
    assert await backend.get_job_result(queue, "j-success") == "done"
    assert (queue, "j-success") not in backend.active_jobs


async def test_handle_job_does_not_repeat_active_transition_for_reserved_payload():
    class ActiveReservationBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.active_updates = 0

        async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
            payload = await super().dequeue(queue_name)
            return {**payload, "status": State.ACTIVE} if payload is not None else None

        async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
            if state == State.ACTIVE:
                self.active_updates += 1
            await super().update_job_state(queue_name, job_id, state)

    backend = ActiveReservationBackend()
    settings.backend = backend

    async def _task() -> str:
        return "done"

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._reserved_payload_success"] = {"func": _task}

    queue = "test_queue_reserved_payload_success"
    job = Job(task_id="tests._reserved_payload_success", args=[], kwargs={}, job_id="j-reserved")
    await backend.enqueue(queue, job.to_dict())
    payload = await backend.dequeue(queue)

    assert payload is not None
    assert payload["status"] == State.ACTIVE

    await handle_job(queue, payload, backend)

    assert backend.active_updates == 0
    assert await backend.get_job_state(queue, "j-reserved") == State.COMPLETED


async def test_handle_job_skips_dependency_resolution_only_when_payload_has_no_dependents():
    class DependencyCountingBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.resolve_calls = 0

        async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
            self.resolve_calls += 1
            await super().resolve_dependency(queue_name, parent_id)

    backend = DependencyCountingBackend()
    settings.backend = backend

    async def _task() -> str:
        return "done"

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._dependency_marker_success"] = {"func": _task}

    queue = "test_queue_dependency_marker_success"
    without_children = Job(
        task_id="tests._dependency_marker_success",
        args=[],
        kwargs={},
        job_id="j-no-children",
    )
    with_children = Job(
        task_id="tests._dependency_marker_success",
        args=[],
        kwargs={},
        job_id="j-with-children",
    )

    await backend.enqueue(queue, {**without_children.to_dict(), "has_dependents": False})
    payload = await backend.dequeue(queue)
    assert payload is not None
    await handle_job(queue, payload, backend)
    assert backend.resolve_calls == 0

    await backend.enqueue(queue, {**with_children.to_dict(), "has_dependents": True})
    payload = await backend.dequeue(queue)
    assert payload is not None
    await handle_job(queue, payload, backend)
    assert backend.resolve_calls == 1


async def test_worker_retry_uses_backend_lifecycle_transition():
    class LifecycleBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.retry_calls = 0

        async def retry_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
            self.retry_calls += 1
            await super().retry_active_job(queue_name, payload, run_at)

        async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
            if state != State.ACTIVE:
                raise AssertionError("worker must retry through retry_active_job")
            await super().update_job_state(queue_name, job_id, state)

        async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
            raise AssertionError("worker must retry through retry_active_job")

        async def ack(self, queue_name: str, job_id: str) -> None:
            raise AssertionError("worker must retry through retry_active_job")

    backend = LifecycleBackend()
    settings.backend = backend

    async def _task() -> None:
        raise RuntimeError("retry me")

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._lifecycle_retry"] = {"func": _task}

    queue = "test_queue_lifecycle_retry"
    job = Job(
        task_id="tests._lifecycle_retry",
        args=[],
        kwargs={},
        job_id="j-retry",
        max_retries=2,
        backoff=0.0,
    )
    await backend.enqueue(queue, job.to_dict())
    payload = await backend.dequeue(queue)

    assert payload is not None

    await handle_job(queue, payload, backend)

    assert backend.retry_calls == 1
    assert await backend.get_job_state(queue, "j-retry") == State.DELAYED
    assert len(backend.delayed[queue]) == 1
    retry_payload = backend.delayed[queue][0][1]
    assert "RuntimeError: retry me" in retry_payload["last_error"]
    assert "RuntimeError: retry me" in retry_payload["error_traceback"]
    assert isinstance(retry_payload["last_attempt"], float)
    assert (queue, "j-retry") not in backend.active_jobs


async def test_worker_failure_uses_backend_lifecycle_transition():
    class LifecycleBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.failure_calls = 0

        async def fail_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
            self.failure_calls += 1
            await super().fail_active_job(queue_name, payload)

        async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
            if state != State.ACTIVE:
                raise AssertionError("worker must fail through fail_active_job")
            await super().update_job_state(queue_name, job_id, state)

        async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
            raise AssertionError("worker must fail through fail_active_job")

        async def ack(self, queue_name: str, job_id: str) -> None:
            raise AssertionError("worker must fail through fail_active_job")

    backend = LifecycleBackend()
    settings.backend = backend

    async def _task() -> None:
        raise RuntimeError("fail me")

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._lifecycle_failure"] = {"func": _task}

    queue = "test_queue_lifecycle_failure"
    job = Job(task_id="tests._lifecycle_failure", args=[], kwargs={}, job_id="j-failed", max_retries=0)
    await backend.enqueue(queue, job.to_dict())
    payload = await backend.dequeue(queue)

    assert payload is not None

    await handle_job(queue, payload, backend)

    assert backend.failure_calls == 1
    assert await backend.get_job_state(queue, "j-failed") == State.FAILED
    assert len(backend.dlqs[queue]) == 1
    failed_payload = backend.dlqs[queue][0]
    assert "RuntimeError: fail me" in failed_payload["last_error"]
    assert "RuntimeError: fail me" in failed_payload["error_traceback"]
    assert isinstance(failed_payload["last_attempt"], float)
    assert (queue, "j-failed") not in backend.active_jobs


async def test_worker_success_clears_stale_failure_metadata():
    backend = InMemoryBackend()
    settings.backend = backend

    async def _task() -> str:
        return "recovered"

    TASK_REGISTRY.clear()
    TASK_REGISTRY["tests._clears_failure_metadata"] = {"func": _task}

    queue = "test_queue_success_clears_failure"
    job = Job(task_id="tests._clears_failure_metadata", args=[], kwargs={}, job_id="j-clean")
    payload = {**job.to_dict(), "last_error": "old failure", "error_traceback": "old traceback"}
    await backend.enqueue(queue, payload)
    raw = await backend.dequeue(queue)

    assert raw is not None

    await handle_job(queue, raw, backend)

    stored = backend.job_payloads[(queue, "j-clean")]
    assert stored["status"] == State.COMPLETED
    assert stored["last_error"] is None
    assert stored["error_traceback"] is None
    assert isinstance(stored["last_attempt"], float)


async def test_worker_renews_job_heartbeat_while_handler_runs():
    class HeartbeatBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.heartbeat_times: list[float] = []

        async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
            self.heartbeat_times.append(timestamp)
            await super().save_heartbeat(queue_name, job_id, timestamp)

    previous = (
        settings.enable_stalled_check,
        settings.stalled_threshold,
        settings.stalled_check_interval,
    )
    settings.enable_stalled_check = True
    settings.stalled_threshold = 0.3
    settings.stalled_check_interval = 0.1

    try:
        backend = HeartbeatBackend()
        settings.backend = backend

        async def _slow_task() -> str:
            await anyio.sleep(0.35)
            return "done"

        TASK_REGISTRY.clear()
        TASK_REGISTRY["tests._heartbeat_renewal"] = {"func": _slow_task}

        queue = "test_queue_heartbeat_renewal"
        job = Job(task_id="tests._heartbeat_renewal", args=[], kwargs={}, job_id="j-heartbeat")
        await backend.enqueue(queue, job.to_dict())
        payload = await backend.dequeue(queue)

        assert payload is not None

        await handle_job(queue, payload, backend)

        assert len(backend.heartbeat_times) >= 2
        assert await backend.get_job_state(queue, "j-heartbeat") == State.COMPLETED
    finally:
        (
            settings.enable_stalled_check,
            settings.stalled_threshold,
            settings.stalled_check_interval,
        ) = previous


async def test_worker_heartbeat_renewal_failure_does_not_cancel_handler():
    class TransientHeartbeatBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.save_calls = 0
            self.failed_once = False

        async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
            self.save_calls += 1
            if self.save_calls == 2:
                self.failed_once = True
                raise RuntimeError("transient heartbeat failure")
            await super().save_heartbeat(queue_name, job_id, timestamp)

    previous = (
        settings.enable_stalled_check,
        settings.stalled_threshold,
        settings.stalled_check_interval,
    )
    settings.enable_stalled_check = True
    settings.stalled_threshold = 0.3
    settings.stalled_check_interval = 0.1

    try:
        backend = TransientHeartbeatBackend()
        settings.backend = backend
        executions = 0

        async def _slow_task() -> str:
            nonlocal executions
            executions += 1
            await anyio.sleep(0.35)
            return "done"

        TASK_REGISTRY.clear()
        TASK_REGISTRY["tests._heartbeat_transient_failure"] = {"func": _slow_task}

        queue = "test_queue_heartbeat_transient_failure"
        job = Job(
            task_id="tests._heartbeat_transient_failure",
            args=[],
            kwargs={},
            job_id="j-heartbeat-transient",
            max_retries=0,
        )
        await backend.enqueue(queue, job.to_dict())
        payload = await backend.dequeue(queue)

        assert payload is not None

        await handle_job(queue, payload, backend)

        stored = backend.job_payloads[(queue, "j-heartbeat-transient")]
        assert backend.failed_once is True
        assert backend.save_calls >= 3
        assert executions == 1
        assert stored["status"] == State.COMPLETED
        assert stored["retries"] == 0
        assert stored["result"] == "done"
    finally:
        (
            settings.enable_stalled_check,
            settings.stalled_threshold,
            settings.stalled_check_interval,
        ) = previous


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
