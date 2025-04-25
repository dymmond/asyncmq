import asyncio
import time
import pytest
from asyncqueue.backends.memory import InMemoryBackend
from asyncqueue.delayed_scanner import delayed_job_scanner
from asyncqueue.job import Job
from asyncqueue.task import task, TASK_REGISTRY
from asyncqueue.worker import handle_job


@task(queue="test")
async def sum_task(values):
    return sum(values)

def get_task_id(func):
    for key, entry in TASK_REGISTRY.items():
        if entry["func"] == func:
            return key
    raise RuntimeError(f"Task {func.__name__} is not registered.")

@pytest.mark.asyncio
async def test_delayed_enqueue_and_scan():
    backend = InMemoryBackend()
    job = Job(task_id="delay.test", args=[1], kwargs={}, ttl=10)
    run_at = time.time() + 0.3
    await backend.enqueue_delayed("test", job.to_dict(), run_at)

    task = asyncio.create_task(delayed_job_scanner("test", backend, interval=0.1))
    await asyncio.sleep(0.5)
    task.cancel()

    result = await backend.dequeue("test")

    assert result["id"] == job.id

@pytest.mark.asyncio
async def test_delayed_job_not_due():
    backend = InMemoryBackend()
    job = Job(task_id="not.due", args=[], kwargs={})
    run_at = time.time() + 5
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    due = await backend.get_due_delayed("test")

    assert job.id not in [j["id"] for j in due]

@pytest.mark.asyncio
async def test_remove_delayed_job():
    backend = InMemoryBackend()
    job = Job(task_id="remove.test", args=[], kwargs={})
    run_at = time.time() + 1
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    await backend.remove_delayed("test", job.id)
    due = await backend.get_due_delayed("test")

    assert job.id not in [j["id"] for j in due]

@pytest.mark.asyncio
async def test_expired_job_detection():
    job = Job(task_id="expire.check", args=[], kwargs={}, ttl=0.1)
    time.sleep(0.2)

    assert job.is_expired()

@pytest.mark.asyncio
async def test_job_moves_to_dlq_when_expired():
    backend = InMemoryBackend()
    job = Job(task_id="expire.dlq", args=[], kwargs={}, ttl=0.1)
    await backend.enqueue("test", job.to_dict())
    await asyncio.sleep(0.2)
    await backend.move_to_dlq("test", job.to_dict())
    state = await backend.get_job_state("test", job.id)

    assert state == "failed"

@pytest.mark.asyncio
async def test_multiple_delayed_jobs():
    backend = InMemoryBackend()
    jobs = [Job(task_id=f"delay.multi.{i}", args=[], kwargs={}) for i in range(5)]
    now = time.time()
    for job in jobs:
        await backend.enqueue_delayed("test", job.to_dict(), now + 0.2)
    await asyncio.sleep(0.3)
    due = await backend.get_due_delayed("test")

    assert len(due) == 5

@pytest.mark.asyncio
async def test_duplicate_delayed_enqueue():
    backend = InMemoryBackend()

    job1 = Job(task_id="delay.dup", args=[], kwargs={})
    job2 = Job(task_id="delay.dup", args=[], kwargs={})

    now = time.time()
    await backend.enqueue_delayed("test", job1.to_dict(), now)
    await backend.enqueue_delayed("test", job2.to_dict(), now)
    due = await backend.get_due_delayed("test")

    assert job1.id in [j["id"] for j in due] or job2.id in [j["id"] for j in due]

@pytest.mark.asyncio
async def test_scan_does_not_repeat():
    backend = InMemoryBackend()
    job = Job(task_id="scan.once", args=[], kwargs={})
    run_at = time.time() + 0.3
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    task = asyncio.create_task(delayed_job_scanner("test", backend, interval=0.1))
    await asyncio.sleep(0.6)
    task.cancel()
    enqueued = await backend.dequeue("test")
    second = await backend.get_due_delayed("test")
    assert enqueued["id"] == job.id

    assert job.id not in [j["id"] for j in second]

@pytest.mark.asyncio
async def test_delayed_enqueue_past_time():
    backend = InMemoryBackend()
    job = Job(task_id="past.enqueue", args=[], kwargs={})
    run_at = time.time() - 5
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    due = await backend.get_due_delayed("test")

    assert any(j["id"] == job.id for j in due)

@pytest.mark.asyncio
async def test_delayed_job_status_marked_delayed():
    backend = InMemoryBackend()
    job = Job(task_id="status.delay", args=[], kwargs={})
    run_at = time.time() + 0.3
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    state = await backend.get_job_state("test", job.id)

    assert state == "delayed"

@pytest.mark.asyncio
async def test_delayed_result_is_none_initially():
    backend = InMemoryBackend()
    job = Job(task_id="delay.result.none", args=[], kwargs={})
    run_at = time.time() + 0.4
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    result = await backend.get_job_result("test", job.id)

    assert result is None


@pytest.mark.asyncio
async def test_delayed_then_execute():
    backend = InMemoryBackend()

    job = Job(task_id=get_task_id(sum_task), args=[[1, 2, 3]], kwargs={})
    run_at = time.time() + 0.2
    await backend.enqueue_delayed("test", job.to_dict(), run_at)

    # Run the scanner to move job into active queue
    scanner = asyncio.create_task(delayed_job_scanner("test", backend, interval=0.1))
    await asyncio.sleep(0.3)
    scanner.cancel()

    # Now the job should be ready for dequeue
    raw = await backend.dequeue("test")
    await handle_job("test", backend, raw)

    result = await backend.get_job_result("test", job.id)
    assert result == 6
