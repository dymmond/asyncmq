import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.core.enums import State
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job
from asyncmq.queues import Queue
from asyncmq.workers import Worker

pytestmark = pytest.mark.anyio

class DummyBackend(InMemoryBackend):
    def __init__(self):
        super().__init__()
        self.paused = False
        self.resumed = False

    async def pause_queue(self, queue_name: str):
        self.paused = True

    async def resume_queue(self, queue_name: str):
        self.resumed = True


async def test_add_job_without_delay():
    backend = InMemoryBackend()
    q = Queue('queue1', backend=backend)

    job_id = await q.add('task1', args=[1], kwargs={'a': 2})
    assert job_id is not None

    raw = await backend.dequeue('queue1')
    assert raw['id'] == job_id
    assert raw['task'] == 'task1'
    assert raw['args'] == [1]
    assert raw['kwargs'] == {'a': 2}


async def test_add_job_with_delay():
    backend = InMemoryBackend()
    q = Queue('queue2', backend=backend)

    job_id = await q.add('task2', delay=0.1)
    assert job_id is not None

    # Should not be in the immediate queue
    assert await backend.dequeue('queue2') is None

    # In delayed storage
    delayed = backend.delayed.get('queue2', [])
    assert any(payload['id'] == job_id for _, payload in delayed)


async def test_add_bulk():
    backend = InMemoryBackend()
    q = Queue('queue3', backend=backend)

    jobs = [
        {'task_id': 't1', 'args': [1]},
        {'task_id': 't2', 'args': [2, 3]},
    ]
    ids = await q.add_bulk(jobs)
    assert len(ids) == 2

    popped = [await backend.dequeue('queue3'), await backend.dequeue('queue3')]
    popped_ids = {p['id'] for p in popped}
    assert set(ids) == popped_ids


def test_add_repeatable():
    backend = InMemoryBackend()
    q = Queue('queue4', backend=backend)

    q.add_repeatable('repeat_task', every=5)
    assert len(q._repeatables) == 1
    rep = q._repeatables[0]
    assert rep['task_id'] == 'repeat_task'
    assert rep['every'] == 5


async def test_pause_resume():
    backend = DummyBackend()
    q = Queue('queue5', backend=backend)

    await q.pause()
    assert backend.paused is True

    await q.resume()
    assert backend.resumed is True


async def test_clean():
    backend = InMemoryBackend()
    q = Queue('queue6', backend=backend)

    job_id = await q.add('task_clean')
    state = await backend.get_job_state('queue6', job_id)
    assert state == State.WAITING

    await q.clean(State.WAITING)
    state2 = await backend.get_job_state('queue6', job_id)
    assert state2 is None


async def test_flowproducer_add_flow(monkeypatch):
    backend = InMemoryBackend()
    fp = FlowProducer(backend=backend)

    job1 = Job('taskA', [], {}, job_id='idA')
    job2 = Job('taskB', [], {}, job_id='idB')
    called = []

    async def fake_deps(b, queue, job):
        called.append((queue, job.id))

    monkeypatch.setattr(fp, '_add_dependencies', fake_deps)

    ids = await fp.add_flow('queue7', [job1, job2])
    assert ids == ['idA', 'idB']

    enq1 = await backend.dequeue('queue7')
    enq2 = await backend.dequeue('queue7')
    assert enq1['id'] == 'idA'
    assert enq2['id'] == 'idB'
    assert called == [('queue7', 'idA'), ('queue7', 'idB')]


def test_worker_interface():
    q = Queue('queue8', backend=InMemoryBackend())
    w = Worker(q)
    assert hasattr(w, 'start')
    assert hasattr(w, 'stop')
