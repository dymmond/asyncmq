import asyncio
import threading
import time

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.event import event_emitter
from asyncmq.flow import FlowProducer
from asyncmq.job import Job
from asyncmq.queue import Queue
from asyncmq.runner import run_worker
from asyncmq.task import list_tasks, task
from asyncmq.worker import Worker

# Utility to reset global registry and listeners
# and ensure no state leaks between tests or modules

def reset_globals():
    list_tasks().clear()
    event_emitter._listeners.clear()

# Clear any residual state from other test modules
# reset_globals()

# Automatically clear before and after each test in this module\@pytest.fixture(autouse=True)
def clear_between_tests():
    reset_globals()
    yield
    reset_globals()

@pytest.mark.asyncio
async def test_real_task_execution():
    @task(queue='test_exec')
    async def add(a, b):
        return a + b

    backend = InMemoryBackend()
    q = Queue('test_exec', backend=backend)

    fut = asyncio.get_event_loop().create_future()
    def on_complete(data):
        if data['task'] == 'add':
            fut.set_result(data)
    event_emitter.on('job:completed', on_complete)

    worker = asyncio.create_task(
        run_worker('test_exec', backend, concurrency=1, rate_limit=None, rate_interval=1.0, repeatables=None)
    )
    job_id = await q.add('add', args=[2, 3])
    data = await asyncio.wait_for(fut, timeout=2)

    assert data['id'] == job_id
    assert data['result'] == 5

    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

@pytest.mark.asyncio
async def test_retries_and_dlq():
    @task(queue='retry', retries=1)
    async def flaky():
        raise RuntimeError("fail")

    backend = InMemoryBackend()
    q = Queue('retry', backend=backend)

    worker = asyncio.create_task(
        run_worker('retry', backend, concurrency=1, rate_limit=None, rate_interval=1.0, repeatables=None)
    )
    job_id = await q.add('flaky')
    await asyncio.sleep(0.5)

    dlq = backend.dlqs.get('retry', [])
    assert any(job['id'] == job_id for job in dlq)

    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

@pytest.mark.asyncio
async def test_ttl_expiration():
    @task(queue='ttl', retries=0)
    async def dummy():
        return "ok"

    backend = InMemoryBackend()
    q = Queue('ttl', backend=backend)

    job_id = await q.add('dummy', ttl=0.01)
    await asyncio.sleep(0.02)

    worker = asyncio.create_task(
        run_worker('ttl', backend, concurrency=1, rate_limit=None, rate_interval=1.0, repeatables=None)
    )
    await asyncio.sleep(0.2)

    expired = backend.dlqs.get('ttl', [])
    assert any(job['status'] == 'expired' and job['id'] == job_id for job in expired)

    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

@pytest.mark.asyncio
async def test_dependencies_flow():
    order = []

    @task(queue='flow', retries=0)
    async def parent():
        order.append('parent')
        return 1

    @task(queue='flow', retries=0)
    async def child(x):
        order.append(('child', x))
        return x + 1

    backend = InMemoryBackend()
    fp = FlowProducer(backend=backend)

    pj = Job('parent', [], {}, job_id='p1')
    cj = Job('child', [], {}, job_id='c1')
    cj.depends_on = ['p1']

    ids = await fp.add_flow('flow', [pj, cj])
    assert ids == ['p1', 'c1']

    worker = asyncio.create_task(
        run_worker('flow', backend, concurrency=1, rate_limit=None, rate_interval=1.0, repeatables=None)
    )
    await asyncio.sleep(0.5)

    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    parent_idx = order.index('parent')
    child_idx = next(i for i, v in enumerate(order) if isinstance(v, tuple))
    assert child_idx > parent_idx

@pytest.mark.asyncio
async def test_rate_limiting():
    timestamps = []

    @task(queue='rate', retries=0)
    async def job_task():
        timestamps.append(time.time())

    backend = InMemoryBackend()
    q = Queue('rate', backend=backend, concurrency=5, rate_limit=1, rate_interval=0.05)

    worker = asyncio.create_task(
        run_worker('rate', backend, concurrency=5, rate_limit=1, rate_interval=0.05, repeatables=None)
    )
    for _ in range(3):
        await q.add('job_task')

    await asyncio.sleep(0.3)
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    diffs = [t2 - t1 for t1, t2 in zip(timestamps, timestamps[1:])]
    assert all(diff >= 0.04 for diff in diffs)

@pytest.mark.asyncio
async def test_repeatables():
    count = 0

    @task(queue='rep', retries=0)
    async def rep_task():
        nonlocal count
        count += 1

    backend = InMemoryBackend()
    q = Queue('rep', backend=backend)
    q.add_repeatable('rep_task', every=0.05)

    worker = asyncio.create_task(
        run_worker('rep', backend, concurrency=1, rate_limit=None, rate_interval=1.0, repeatables=q._repeatables)
    )
    await asyncio.sleep(0.3)

    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)
    assert count >= 4


def test_worker_start_stop_threaded():
    # No tasks registered here—just worker threading behavior
    class FastQueue(Queue):
        async def run(self):
            await asyncio.sleep(0.01)

    fq = FastQueue('fq', backend=InMemoryBackend())
    w = Worker(fq)
    thread = threading.Thread(target=w.start)
    thread.start()
    thread.join(timeout=1)
    assert not thread.is_alive()

    class InfiniteQueue(Queue):
        async def run(self):
            while True:
                await asyncio.sleep(0.1)

    iq = InfiniteQueue('iq', backend=InMemoryBackend())
    w2 = Worker(iq)
    thread2 = threading.Thread(target=w2.start)
    thread2.start()
    time.sleep(0.2)
    w2.stop()
    thread2.join(timeout=1)
    assert not thread2.is_alive()
