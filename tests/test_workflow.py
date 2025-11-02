import anyio
import pytest
from anyio import CapacityLimiter

import asyncmq
from asyncmq.backends.memory import InMemoryBackend
from asyncmq.core.delayed_scanner import delayed_job_scanner
from asyncmq.core.enums import State
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job
from asyncmq.queues import Queue
from asyncmq.tasks import task
from asyncmq.workers import Worker, process_job

pytestmark = pytest.mark.anyio

EVENTS = []


class DummyBackend(InMemoryBackend):
    def __init__(self):
        super().__init__()
        self.paused = False
        self.resumed = False

    async def pause_queue(self, queue_name: str):
        self.paused = True

    async def resume_queue(self, queue_name: str):
        self.resumed = True


class AtomicBackendStub:
    """
    Minimal backend exposing atomic_add_flow to verify that FlowProducer
    prefers atomic path and does NOT call enqueue/_add_dependencies itself.
    """

    def __init__(self):
        self.called = []

    async def atomic_add_flow(self, queue, payloads, deps):
        # record call and return ids in the incoming order
        self.called.append(("atomic_add_flow", queue, [p["id"] for p in payloads], list(deps)))
        return [p["id"] for p in payloads]


async def test_add_job_without_delay():
    backend = InMemoryBackend()
    q = Queue("queue1", backend=backend)

    job_id = await q.add("task1", args=[1], kwargs={"a": 2})
    assert job_id is not None

    raw = await backend.dequeue("queue1")
    assert raw["id"] == job_id
    assert raw["task"] == "task1"
    assert raw["args"] == [1]
    assert raw["kwargs"] == {"a": 2}


async def test_add_job_with_delay():
    backend = InMemoryBackend()
    q = Queue("queue2", backend=backend)

    job_id = await q.add("task2", delay=0.1)
    assert job_id is not None

    # Should not be in the immediate queue
    assert await backend.dequeue("queue2") is None

    # In delayed storage
    delayed = backend.delayed.get("queue2", [])
    assert any(payload["id"] == job_id for _, payload in delayed)


async def test_add_bulk():
    backend = InMemoryBackend()
    q = Queue("queue3", backend=backend)

    jobs = [
        {"task_id": "t1", "args": [1]},
        {"task_id": "t2", "args": [2, 3]},
    ]
    ids = await q.add_bulk(jobs)
    assert len(ids) == 2

    popped = [await backend.dequeue("queue3"), await backend.dequeue("queue3")]
    popped_ids = {p["id"] for p in popped}
    assert set(ids) == popped_ids


def test_add_repeatable():
    backend = InMemoryBackend()
    q = Queue("queue4", backend=backend)

    q.add_repeatable("repeat_task", every=5)
    assert len(q._repeatables) == 1
    rep = q._repeatables[0]
    assert rep["task_id"] == "repeat_task"
    assert rep["every"] == 5


async def test_pause_resume():
    backend = DummyBackend()
    q = Queue("queue5", backend=backend)

    await q.pause()
    assert backend.paused is True

    await q.resume()
    assert backend.resumed is True


async def test_clean():
    backend = InMemoryBackend()
    q = Queue("queue6", backend=backend)

    job_id = await q.add("task_clean")
    state = await backend.get_job_state("queue6", job_id)
    assert state == State.WAITING

    await q.clean(State.WAITING)
    state2 = await backend.get_job_state("queue6", job_id)
    assert state2 is None


async def test_add_flow_roots_only(monkeypatch):
    backend = InMemoryBackend()
    fp = FlowProducer(backend=backend)

    job1 = Job("taskA", [], {}, job_id="idA")
    job2 = Job("taskB", [], {}, job_id="idB")

    async def fail_if_called(*_, **__):
        pytest.fail("_add_dependencies should not be called for root jobs")

    # Roots have no deps → must NOT be called
    monkeypatch.setattr(fp, "_add_dependencies", fail_if_called)

    ids = await fp.add_flow("queue7", [job1, job2])
    assert ids == ["idA", "idB"]

    enq1 = await backend.dequeue("queue7")
    enq2 = await backend.dequeue("queue7")
    assert enq1["id"] == "idA"
    assert enq2["id"] == "idB"


def test_worker_interface():
    q = Queue("queue8", backend=InMemoryBackend())
    w = Worker(q)
    assert hasattr(w, "start")
    assert hasattr(w, "stop")


async def test_add_flow_deps_before_enqueue_in_fallback(monkeypatch):
    memory_backend = InMemoryBackend()
    flow_producer = FlowProducer(backend=memory_backend)

    parent_job = Job("taskA", [], {}, job_id="idA")
    child_job = Job("taskB", [], {}, job_id="idB", depends_on=["idA"])

    event_order = []

    # Correct signature (queue, job, backend)
    async def record_add_dependencies(queue, job, backend):
        event_order.append(("deps", queue, job.id))

    orig_enqueue = memory_backend.enqueue

    async def record_enqueue(queue, payload):
        event_order.append(("enqueue", queue, payload["id"]))
        return await orig_enqueue(queue, payload)

    # Force fallback
    async def _not_impl(*args, **kwargs):
        raise NotImplementedError

    monkeypatch.setattr(InMemoryBackend, "atomic_add_flow", _not_impl, raising=False)

    # Patch both FlowProducer hook and module function
    monkeypatch.setattr(flow_producer, "_add_dependencies", record_add_dependencies, raising=False)
    monkeypatch.setattr("asyncmq.core.dependencies.add_dependencies", record_add_dependencies, raising=False)

    monkeypatch.setattr(memory_backend, "enqueue", record_enqueue, raising=True)

    ids = await flow_producer.add_flow("queue_fallback", [parent_job, child_job])

    assert ids == ["idA", "idB"]

    # Expect enqueue root, then deps(child), then enqueue child
    assert event_order == [
        ("enqueue", "queue_fallback", "idA"),
        ("deps", "queue_fallback", "idB"),
        ("enqueue", "queue_fallback", "idB"),
    ]


async def test_add_flow_uses_atomic_when_available(monkeypatch):
    backend = AtomicBackendStub()
    fp = FlowProducer(backend=backend)

    jobA = Job("taskA", [], {}, job_id="idA")
    jobB = Job("taskB", [], {}, job_id="idB", depends_on=["idA"])

    # If fallback methods were called, we'd detect it by forcing a fail on _add_dependencies
    async def fail_if_called(*_, **__):
        pytest.fail("_add_dependencies must not be called when atomic path is available")

    monkeypatch.setattr(fp, "_add_dependencies", fail_if_called)

    ids = await fp.add_flow("queue_atomic", [jobA, jobB])

    assert ids == ["idA", "idB"]

    # Verify atomic path was used once with correct payload order and deps
    assert backend.called and backend.called[0][0] == "atomic_add_flow"

    _, qname, ids_sent, deps = backend.called[0]

    assert qname == "queue_atomic"
    assert ids_sent == ["idA", "idB"]
    assert deps == [("idA", "idB")]


@task(queue="test_pipeline_mem")
async def ingest_mem(source: str):
    EVENTS.append("INGEST:start")
    await anyio.sleep(0.05)
    EVENTS.append("INGEST:done")
    return {"status": "ingested"}


@task(queue="test_pipeline_mem")
async def transform_mem(threshold: float):
    EVENTS.append(f"TRANSFORM{int(threshold)}:start")
    await anyio.sleep(0.05 * threshold)
    EVENTS.append(f"TRANSFORM{int(threshold)}:done")
    return {"status": "transformed"}


@task(queue="test_pipeline_mem")
async def load_mem(dest: str):
    EVENTS.append("LOAD:start")
    await anyio.sleep(0.05)
    EVENTS.append("LOAD:done")
    return {"status": "loaded"}


async def test_parallel_dag_execution(redis, monkeypatch):
    EVENTS.clear()
    monkeypatch.setattr(asyncmq.monkay.settings, "sandbox_enabled", False, raising=False)

    backend = InMemoryBackend()

    flow_producer = FlowProducer(backend=backend)

    job_ingest = Job(
        task_id=f"{ingest_mem.__module__}.{ingest_mem.__name__}",
        args=["source"],
        kwargs={},
        job_id="job_ingest",
    )
    job_transform_fast = Job(
        task_id=f"{transform_mem.__module__}.{transform_mem.__name__}",
        args=[],
        kwargs={"threshold": 1},
        depends_on=[job_ingest.id],
        job_id="job_transform_fast",
    )
    job_transform_slow = Job(
        task_id=f"{transform_mem.__module__}.{transform_mem.__name__}",
        args=[],
        kwargs={"threshold": 3},
        depends_on=[job_ingest.id],
        job_id="job_transform_slow",
    )
    job_load = Job(
        task_id=f"{load_mem.__module__}.{load_mem.__name__}",
        args=["destination"],
        kwargs={},
        depends_on=[job_transform_fast.id, job_transform_slow.id],
        job_id="job_load",
    )

    job_ids = await flow_producer.add_flow(
        "test_pipeline_mem",
        [job_ingest, job_transform_fast, job_transform_slow, job_load],
    )
    assert job_ids == ["job_ingest", "job_transform_fast", "job_transform_slow", "job_load"]

    # Seed the root (in case atomic_add_flow only stored deps)
    await backend.enqueue("test_pipeline_mem", job_ingest.to_dict())

    # Run worker + delayed scanner. Stop when LOAD finishes
    async with anyio.create_task_group() as tg:
        tg.start_soon(process_job, "test_pipeline_mem", CapacityLimiter(3), None, backend)
        tg.start_soon(delayed_job_scanner, "test_pipeline_mem", backend, 0.02)

        with anyio.fail_after(5.0):
            while "LOAD:done" not in EVENTS:
                await anyio.sleep(0.02)
        tg.cancel_scope.cancel()

    # Ordering checks
    assert "INGEST:start" in EVENTS and "INGEST:done" in EVENTS
    assert EVENTS.index("INGEST:done") < EVENTS.index("TRANSFORM1:start")
    assert EVENTS.index("INGEST:done") < EVENTS.index("TRANSFORM3:start")
    assert EVENTS.index("TRANSFORM1:done") < EVENTS.index("LOAD:start")
    assert EVENTS.index("TRANSFORM3:done") < EVENTS.index("LOAD:start")
    assert EVENTS[-1] == "LOAD:done"
