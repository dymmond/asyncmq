import asyncio
import logging
from contextlib import suppress

import anyio
import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.jobs import Job
from asyncmq.runners import run_worker
from asyncmq.tasks import TASK_REGISTRY, task

pytestmark = pytest.mark.anyio


def get_task_id(func):
    for key, entry in TASK_REGISTRY.items():
        if entry["func"] == func:
            return key
    raise RuntimeError(f"Task {func.__name__} is not registered.")


@task(queue="runner")
async def hello():
    return "hi"


@task(queue="runner")
async def raise_error():
    raise RuntimeError("fail")


@task(queue="runner")
async def echo(value):
    return value


@task(queue="runner")
async def add(x, y):
    return x + y


@task(queue="runner")
async def fail_once_then_succeed():
    if not hasattr(fail_once_then_succeed, "called"):
        fail_once_then_succeed.called = True
        raise RuntimeError("fail once")
    return "success"


@task(queue="runner")
async def fails():
    raise RuntimeError("This is a problem")


async def test_run_worker_success():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(hello), args=[], kwargs={})
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    result = await backend.get_job_result("runner", job.id)
    state = await backend.get_job_state("runner", job.id)

    assert state == State.COMPLETED
    assert result == "hi"


async def test_run_worker_failure_goes_to_dlq():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(raise_error), args=[], kwargs={}, max_retries=0)
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    state = await backend.get_job_state("runner", job.id)
    assert state == State.FAILED


async def test_echo_value():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(echo), args=["hello world"], kwargs={})
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    result = await backend.get_job_result("runner", job.id)
    assert result == "hello world"


async def test_addition_task():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(add), args=[3, 4], kwargs={})
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    result = await backend.get_job_result("runner", job.id)
    assert result == 7


async def test_fail_then_succeed():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(fail_once_then_succeed), args=[], kwargs={}, backoff=0.1)
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(1.5)
    worker.cancel()

    result = await backend.get_job_result("runner", job.id)
    state = await backend.get_job_state("runner", job.id)
    assert state == State.COMPLETED
    assert result == "success"


async def test_worker_cancels_gracefully():
    backend = InMemoryBackend()
    worker = asyncio.create_task(run_worker("runner", backend=backend))

    worker.cancel()
    await asyncio.sleep(0.3)

    assert worker.cancelled() or worker.done()


async def test_job_ttl_expires():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(hello), args=[], kwargs={}, ttl=0.1)
    await backend.enqueue("runner", job.to_dict())

    # Wait for job to expire before worker starts
    await asyncio.sleep(0.2)

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.3)
    worker.cancel()

    state = await backend.get_job_state("runner", job.id)
    assert state in {State.EXPIRED, State.FAILED}


async def test_worker_handles_multiple_jobs():
    backend = InMemoryBackend()

    job_ids = []
    for i in range(5):
        job = Job(task_id=get_task_id(echo), args=[f"msg-{i}"], kwargs={})
        await backend.enqueue("runner", job.to_dict())
        job_ids.append(job.id)

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(2)
    worker.cancel()

    for job_id in job_ids:
        state = await backend.get_job_state("runner", job_id)
        assert state == State.COMPLETED


async def test_run_worker_concurrency_one_reserves_only_one_executable_job():
    backend = InMemoryBackend()
    started = anyio.Event()
    release = anyio.Event()
    started_count = 0

    @task(queue="runner")
    async def blocking_prefetch_task(value):
        nonlocal started_count
        started_count += 1
        started.set()
        await release.wait()
        return value

    task_id = get_task_id(blocking_prefetch_task)
    for index in range(25):
        job = Job(task_id=task_id, args=[index], kwargs={}, job_id=f"prefetch-{index}")
        await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(
        run_worker("runner", backend=backend, concurrency=1, rate_limit=None, rate_interval=1.0, repeatables=None)
    )
    try:
        with anyio.fail_after(2.0):
            await started.wait()
        await anyio.sleep(0.2)

        active_ids = [job_id for queue_name, job_id in backend.active_jobs if queue_name == "runner"]
        assert active_ids == ["prefetch-0"]
        assert len(backend.queues.get("runner", [])) == 24
        assert started_count == 1
    finally:
        release.set()
        await anyio.sleep(0.05)
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker


async def test_run_worker_reservations_never_exceed_concurrency():
    backend = InMemoryBackend()
    release = anyio.Event()
    third_started = anyio.Event()
    started_count = 0

    @task(queue="runner")
    async def concurrent_blocking_task(value):
        nonlocal started_count
        started_count += 1
        if started_count == 3:
            third_started.set()
        await release.wait()
        return value

    task_id = get_task_id(concurrent_blocking_task)
    for index in range(10):
        job = Job(task_id=task_id, args=[index], kwargs={}, job_id=f"bounded-{index}")
        await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(
        run_worker("runner", backend=backend, concurrency=3, rate_limit=None, rate_interval=1.0, repeatables=None)
    )
    try:
        with anyio.fail_after(2.0):
            await third_started.wait()
        await anyio.sleep(0.2)

        active_ids = [job_id for queue_name, job_id in backend.active_jobs if queue_name == "runner"]
        assert active_ids == ["bounded-0", "bounded-1", "bounded-2"]
        assert len(backend.queues.get("runner", [])) == 7
        assert started_count == 3
    finally:
        release.set()
        await anyio.sleep(0.05)
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker


async def test_rate_limited_worker_reserves_only_when_token_is_available():
    backend = InMemoryBackend()
    started = anyio.Event()
    release = anyio.Event()
    started_count = 0

    @task(queue="runner")
    async def rate_limited_blocking_task(value):
        nonlocal started_count
        started_count += 1
        started.set()
        await release.wait()
        return value

    task_id = get_task_id(rate_limited_blocking_task)
    for index in range(3):
        job = Job(task_id=task_id, args=[index], kwargs={}, job_id=f"rate-reserve-{index}")
        await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(
        run_worker("runner", backend=backend, concurrency=3, rate_limit=1, rate_interval=5.0, repeatables=None)
    )
    try:
        with anyio.fail_after(2.0):
            await started.wait()
        await anyio.sleep(0.2)

        active_ids = [job_id for queue_name, job_id in backend.active_jobs if queue_name == "runner"]
        waiting_ids = {job["id"] for job in backend.queues.get("runner", [])}
        assert active_ids == ["rate-reserve-0"]
        assert waiting_ids == {"rate-reserve-1", "rate-reserve-2"}
        assert started_count == 1
    finally:
        release.set()
        await anyio.sleep(0.05)
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker


async def test_waiting_jobs_remain_available_to_other_workers_when_one_worker_is_full():
    backend = InMemoryBackend()
    release = anyio.Event()
    first_started = anyio.Event()
    second_started = anyio.Event()
    started: list[str] = []

    @task(queue="runner")
    async def shared_queue_blocking_task(value):
        started.append(value)
        if value == "first":
            first_started.set()
        if value == "second":
            second_started.set()
        await release.wait()
        return value

    task_id = get_task_id(shared_queue_blocking_task)
    await backend.enqueue("runner", Job(task_id=task_id, args=["first"], kwargs={}, job_id="shared-1").to_dict())
    await backend.enqueue("runner", Job(task_id=task_id, args=["second"], kwargs={}, job_id="shared-2").to_dict())

    worker_one = asyncio.create_task(
        run_worker("runner", backend=backend, concurrency=1, rate_limit=None, rate_interval=1.0, repeatables=None)
    )
    try:
        with anyio.fail_after(2.0):
            await first_started.wait()
        assert await backend.get_job_state("runner", "shared-2") == State.WAITING

        worker_two = asyncio.create_task(
            run_worker("runner", backend=backend, concurrency=1, rate_limit=None, rate_interval=1.0, repeatables=None)
        )
        try:
            with anyio.fail_after(2.0):
                await second_started.wait()
            assert set(started) == {"first", "second"}
            assert len([job_id for queue_name, job_id in backend.active_jobs if queue_name == "runner"]) == 2
        finally:
            worker_two.cancel()
            with suppress(asyncio.CancelledError):
                await worker_two
    finally:
        release.set()
        await anyio.sleep(0.05)
        worker_one.cancel()
        with suppress(asyncio.CancelledError):
            await worker_one


async def test_worker_respects_backoff():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(raise_error), args=[], kwargs={}, max_retries=2, backoff=0.2)
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(2)
    worker.cancel()

    state = await backend.get_job_state("runner", job.id)
    assert state == State.FAILED


async def test_run_worker_starts_stalled_recovery_when_enabled(monkeypatch):
    backend = InMemoryBackend()
    started = anyio.Event()

    async def fake_stalled_recovery_scheduler(scheduler_backend):
        assert scheduler_backend is backend
        started.set()
        await anyio.sleep_forever()

    monkeypatch.setattr("asyncmq.runners.stalled_recovery_scheduler", fake_stalled_recovery_scheduler)

    previous = settings.enable_stalled_check
    settings.enable_stalled_check = True
    try:
        worker = asyncio.create_task(run_worker("runner", backend=backend))
        with anyio.fail_after(1.0):
            await started.wait()
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker
    finally:
        settings.enable_stalled_check = previous


async def test_run_worker_renews_worker_heartbeat():
    backend = InMemoryBackend()
    previous_heartbeat_ttl = settings.heartbeat_ttl
    settings.heartbeat_ttl = 0.05
    worker = asyncio.create_task(run_worker("runner", backend=backend))

    try:
        with anyio.fail_after(1.0):
            while not await backend.list_workers():
                await anyio.sleep(0.01)

        initial = (await backend.list_workers())[0].heartbeat

        with anyio.fail_after(1.0):
            while True:
                current = (await backend.list_workers())[0].heartbeat
                if current > initial:
                    break
                await anyio.sleep(0.01)
    finally:
        settings.heartbeat_ttl = previous_heartbeat_ttl
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker

    assert await backend.list_workers() == []


async def test_run_worker_heartbeat_renewal_failure_does_not_stop_worker():
    class FlakyWorkerHeartbeatBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__()
            self.register_calls = 0

        async def register_worker(
            self,
            worker_id: str,
            queue: str,
            concurrency: int,
            timestamp: float,
        ) -> None:
            self.register_calls += 1
            if self.register_calls == 2:
                raise RuntimeError("temporary worker heartbeat outage")
            await super().register_worker(worker_id, queue, concurrency, timestamp)

    backend = FlakyWorkerHeartbeatBackend()
    previous_heartbeat_ttl = settings.heartbeat_ttl
    settings.heartbeat_ttl = 0.05
    worker = asyncio.create_task(run_worker("runner", backend=backend))

    try:
        with anyio.fail_after(1.0):
            while backend.register_calls < 3:
                await anyio.sleep(0.01)

        workers = await backend.list_workers()
        assert len(workers) == 1
        assert workers[0].queue == "runner"
    finally:
        settings.heartbeat_ttl = previous_heartbeat_ttl
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker

    assert await backend.list_workers() == []


async def test_worker_skips_expired_jobs():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(hello), args=[], kwargs={}, ttl=0.1)
    await backend.enqueue("runner", job.to_dict())
    await asyncio.sleep(0.3)

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    state = await backend.get_job_state("runner", job.id)
    assert state == State.FAILED or state == State.EXPIRED


async def test_worker_can_recover_from_job_exception():
    backend = InMemoryBackend()
    job1 = Job(task_id=get_task_id(raise_error), args=[], kwargs={}, max_retries=0)
    job2 = Job(task_id=get_task_id(hello), args=[], kwargs={})
    await backend.enqueue("runner", job1.to_dict())
    await backend.enqueue("runner", job2.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(1)
    worker.cancel()

    state1 = await backend.get_job_state("runner", job1.id)
    state2 = await backend.get_job_state("runner", job2.id)
    assert state1 == State.FAILED
    assert state2 == State.COMPLETED


async def test_worker_handles_zero_args():
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(hello), args=[], kwargs={})
    await backend.enqueue("runner", job.to_dict())
    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()
    result = await backend.get_job_result("runner", job.id)
    assert result == "hi"


async def test_worker_handles_kwargs():
    @task(queue="runner")
    async def greet(name="world"):
        return f"Hello {name}"

    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(greet), args=[], kwargs={"name": "Tarsil"})
    await backend.enqueue("runner", job.to_dict())

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(0.5)
    worker.cancel()

    result = await backend.get_job_result("runner", job.id)
    assert result == "Hello Tarsil"


async def test_worker_long_chain_jobs():
    backend = InMemoryBackend()
    # Enqueue 10 jobs and track their IDs
    job_ids = []
    for i in range(10):
        job = Job(task_id=get_task_id(echo), args=[f"chain-{i}"], kwargs={})
        await backend.enqueue("runner", job.to_dict())
        job_ids.append(job.id)

    worker = asyncio.create_task(run_worker("runner", backend=backend))
    await asyncio.sleep(3)
    worker.cancel()

    for job_id in job_ids:
        state = await backend.get_job_state("runner", job_id)
        assert state == State.COMPLETED


async def test_worker_logs_exceptions():
    def _setup_custom_logging():
        class TestHandler(logging.Handler):
            def emit(self, record):
                logs.append(record)

        logger = logging.getLogger("asyncmq")
        handler = TestHandler()
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    async def run_test():
        async with anyio.create_task_group() as tg:
            tg.start_soon(run_worker, "runner", backend)
            await anyio.sleep(0.5)
            tg.cancel_scope.cancel()

    logs = []
    _setup_custom_logging()
    backend = InMemoryBackend()
    job = Job(task_id=get_task_id(fails), args=[], kwargs={})
    await backend.enqueue("runner", job.to_dict())

    await run_test()

    # Verify the job reached a terminal or retry-delay state within this short window.
    state = await backend.get_job_state("runner", job.id)
    assert state in {State.FAILED, State.EXPIRED, State.DELAYED}

    assert any("This is a problem" in record.getMessage() for record in logs)
    assert any("failed with exception" in record.getMessage() for record in logs)


async def test_registers_worker():
    backend = InMemoryBackend()
    worker = asyncio.create_task(
        run_worker(
            "test_registry", backend=backend, concurrency=2, rate_limit=None, rate_interval=1.0, repeatables=None
        )
    )
    await asyncio.sleep(0.2)

    workers = await backend.list_workers()

    assert len(workers) == 1
    assert workers[0].queue == "test_registry"

    # Clean up
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)


async def test_run_worker_drain_event_exits_after_inflight_job_finishes():
    backend = InMemoryBackend()
    started = anyio.Event()
    release = anyio.Event()
    drain_event = anyio.Event()

    @task(queue="runner")
    async def drainable_task():
        started.set()
        await release.wait()
        return "done"

    job1 = Job(task_id=get_task_id(drainable_task), args=[], kwargs={}, job_id="j1")
    job2 = Job(task_id=get_task_id(drainable_task), args=[], kwargs={}, job_id="j2")
    await backend.enqueue("runner", job1.to_dict())
    await backend.enqueue("runner", job2.to_dict())

    async def run_draining_worker() -> None:
        await run_worker(
            "runner",
            backend=backend,
            concurrency=1,
            rate_limit=None,
            rate_interval=1.0,
            repeatables=None,
            drain_event=drain_event,
        )

    with anyio.fail_after(2.0):
        async with anyio.create_task_group() as tg:
            tg.start_soon(run_draining_worker)
            await started.wait()
            drain_event.set()
            await anyio.sleep(0.05)

            assert await backend.get_job_state("runner", "j2") == State.WAITING

            release.set()

    assert await backend.get_job_state("runner", "j1") == State.COMPLETED
    assert await backend.get_job_state("runner", "j2") == State.WAITING
    assert await backend.list_workers() == []
