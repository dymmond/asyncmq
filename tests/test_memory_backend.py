import asyncio
import time

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.core.enums import State
from asyncmq.job import Job

pytestmark = pytest.mark.anyio

async def test_enqueue_and_dequeue():
    backend = InMemoryBackend()
    job = Job(task_id="test.task", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    result = await backend.dequeue("test")
    assert result["id"] == job.id



async def test_job_state_tracking():
    backend = InMemoryBackend()
    job = Job(task_id="state.test", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    await backend.update_job_state("test", job.id, State.ACTIVE)
    state = await backend.get_job_state("test", job.id)
    assert state == State.ACTIVE



async def test_save_and_get_job_result():
    backend = InMemoryBackend()
    job = Job(task_id="result.test", args=[], kwargs={})
    await backend.enqueue("test", job.to_dict())
    await backend.save_job_result("test", job.id, 1234)
    result = await backend.get_job_result("test", job.id)
    assert result == 1234



async def test_enqueue_delayed_and_get_due():
    backend = InMemoryBackend()
    job = Job(task_id="delay.test", args=[], kwargs={})
    run_at = time.time() + 0.2
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    await asyncio.sleep(0.25)
    due = await backend.get_due_delayed("test")
    assert any(j["id"] == job.id for j in due)



async def test_move_to_dlq():
    backend = InMemoryBackend()
    job = Job(task_id="dlq.test", args=[], kwargs={})
    await backend.move_to_dlq("test", job.to_dict())
    # no direct getter, so we check state
    state = await backend.get_job_state("test", job.id)
    assert state == State.FAILED



async def test_remove_delayed():
    backend = InMemoryBackend()
    job = Job(task_id="delay.remove", args=[], kwargs={})
    run_at = time.time() + 1
    await backend.enqueue_delayed("test", job.to_dict(), run_at)
    await backend.remove_delayed("test", job.id)
    delayed = await backend.get_due_delayed("test")
    assert all(j["id"] != job.id for j in delayed)
