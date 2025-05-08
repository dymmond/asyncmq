import time

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.core.enums import State

pytestmark = pytest.mark.anyio


@pytest.fixture
def backend():
    return InMemoryBackend()


async def test_cancel_job(backend):
    job = {"id": "j1"}
    backend.queues.setdefault("q1", []).append(job.copy())
    backend.delayed.setdefault("q1", []).append((time.time(), job.copy()))

    await backend.cancel_job("q1", "j1")

    assert all(j["id"] != "j1" for j in backend.queues.get("q1", []))
    assert all(j["id"] != "j1" for _, j in backend.delayed.get("q1", []))
    assert await backend.is_job_cancelled("q1", "j1")


async def test_retry_job(backend):
    job = {"id": "j2"}
    backend.dlqs.setdefault("q1", []).append(job.copy())

    result = await backend.retry_job("q1", "j2")

    assert result is True
    assert any(j["id"] == "j2" for j in backend.queues.get("q1", []))
    assert all(j["id"] != "j2" for j in backend.dlqs.get("q1", []))


async def test_remove_job(backend):
    job = {"id": "j3"}
    backend.queues.setdefault("q1", []).append(job.copy())
    backend.delayed.setdefault("q1", []).append((time.time(), job.copy()))
    backend.dlqs.setdefault("q1", []).append(job.copy())
    backend.job_states[("q1", "j3")] = State.ACTIVE

    result = await backend.remove_job("q1", "j3")

    assert result is True
    assert all(j["id"] != "j3" for j in backend.queues.get("q1", []))
    assert all(j["id"] != "j3" for _, j in backend.delayed.get("q1", []))
    assert all(j["id"] != "j3" for j in backend.dlqs.get("q1", []))
    assert ("q1", "j3") not in backend.job_states
