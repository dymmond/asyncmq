"""Benchmarks for InMemoryBackend operations."""

import time

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.core.enums import State
from asyncmq.jobs import Job


@pytest.mark.benchmark
@pytest.mark.anyio
async def test_enqueue_single_job():
    """Benchmark enqueueing a single job."""
    backend = InMemoryBackend()
    job = Job(task_id="bench.task", args=[1], kwargs={})
    await backend.enqueue("bench-queue", job.to_dict())


@pytest.mark.benchmark
@pytest.mark.anyio
async def test_enqueue_and_dequeue():
    """Benchmark enqueue followed by dequeue."""
    backend = InMemoryBackend()
    job = Job(task_id="bench.task", args=[1], kwargs={})
    await backend.enqueue("bench-queue", job.to_dict())
    result = await backend.dequeue("bench-queue")
    assert result is not None


@pytest.mark.benchmark
@pytest.mark.anyio
async def test_enqueue_batch():
    """Benchmark enqueueing a batch of 50 jobs."""
    backend = InMemoryBackend()
    jobs = [Job(task_id="bench.batch_task", args=[i], kwargs={}).to_dict() for i in range(50)]
    for payload in jobs:
        await backend.enqueue("bench-queue", payload)


@pytest.mark.benchmark
@pytest.mark.anyio
async def test_get_job_state():
    """Benchmark getting a job state after enqueue."""
    backend = InMemoryBackend()
    job = Job(task_id="bench.task", args=[], kwargs={})
    payload = job.to_dict()
    await backend.enqueue("bench-queue", payload)
    state = await backend.get_job_state("bench-queue", job.id)
    assert state == "waiting"


@pytest.mark.benchmark
@pytest.mark.anyio
async def test_ack_job():
    """Benchmark the full lifecycle: enqueue, dequeue, update state, save result, ack."""
    backend = InMemoryBackend()
    job = Job(task_id="bench.task", args=[], kwargs={})
    payload = job.to_dict()
    await backend.enqueue("bench-queue", payload)
    await backend.dequeue("bench-queue")
    await backend.update_job_state("bench-queue", job.id, State.COMPLETED)
    await backend.save_job_result("bench-queue", job.id, result="done")
    await backend.ack("bench-queue", job.id)


@pytest.mark.benchmark
@pytest.mark.anyio
async def test_queue_stats():
    """Benchmark checking queue stats after multiple enqueues."""
    backend = InMemoryBackend()
    for i in range(100):
        job = Job(task_id="bench.task", args=[i], kwargs={})
        await backend.enqueue("bench-queue", job.to_dict())
    stats = await backend.queue_stats("bench-queue")
    assert stats["waiting"] == 100


@pytest.mark.benchmark
@pytest.mark.anyio
async def test_enqueue_delayed():
    """Benchmark enqueueing a delayed job."""
    backend = InMemoryBackend()
    job = Job(task_id="bench.delayed_task", args=[], kwargs={})
    run_at = time.time() + 3600
    await backend.enqueue_delayed("bench-queue", job.to_dict(), run_at)


@pytest.mark.benchmark
@pytest.mark.anyio
async def test_move_to_dlq():
    """Benchmark moving a failed job to the dead letter queue."""
    backend = InMemoryBackend()
    job = Job(task_id="bench.failing_task", args=[], kwargs={})
    payload = job.to_dict()
    await backend.enqueue("bench-queue", payload)
    await backend.dequeue("bench-queue")
    await backend.move_to_dlq("bench-queue", payload)


@pytest.mark.benchmark
@pytest.mark.anyio
async def test_bulk_enqueue():
    """Benchmark bulk enqueueing 50 jobs."""
    backend = InMemoryBackend()
    jobs = [Job(task_id="bench.bulk_task", args=[i], kwargs={}).to_dict() for i in range(50)]
    await backend.bulk_enqueue("bench-queue", jobs)
