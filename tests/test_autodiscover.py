import importlib
from types import ModuleType

import pytest

from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.tasks import TASK_REGISTRY
from asyncmq.workers import autodiscover_tasks, handle_job


class DummyBackend:
    def __init__(self):
        self.calls = {
            "update_job_state": [],
            "save_job_result": [],
            "ack": [],
            "emit_events": [],
        }

    async def is_job_cancelled(self, queue, job_id):
        return False

    async def update_job_state(self, queue, job_id, state):
        self.calls["update_job_state"].append((queue, job_id, state))

    async def save_job_result(self, queue, job_id, result):
        self.calls["save_job_result"].append((queue, job_id, result))

    async def ack(self, queue, job_id):
        self.calls["ack"].append((queue, job_id))

    async def save_heartbeat(self, *args, **kwargs):
        pass

    async def enqueue_delayed(self, *args, **kwargs):
        pass

    async def move_to_dlq(self, *args, **kwargs):
        pass


@pytest.fixture(autouse=True)
def stub_event_emitter(monkeypatch):
    class E:
        async def emit(self, *args, **kwargs):
            return None

    monkeypatch.setattr("asyncmq.core.event.event_emitter", E())
    return E()


@pytest.fixture(autouse=True)
def disable_sandbox(monkeypatch):
    monkeypatch.setattr(settings, "sandbox_enabled", False)
    return settings


@pytest.mark.asyncio
async def test_handle_job_import_fallback(monkeypatch):
    """
    If TASK_REGISTRY is missing the task, handle_job should call
    importlib.import_module(task_id) once and then succeed.
    """
    TASK_REGISTRY.clear()

    async def dummy_handler(*args, **kwargs):
        return "ok"

    imported = []

    def fake_import(name):
        TASK_REGISTRY[name] = {"func": dummy_handler}
        imported.append(name)
        return ModuleType(name)

    monkeypatch.setattr(importlib, "import_module", fake_import)

    task_id = "my.dynamic.task"
    raw_job = {
        "id": "j1",
        "task_id": task_id,
        "args": [],
        "kwargs": {},
    }

    class DummyJob:
        def __init__(self, d):
            self.id = d["id"]
            self.task_id = d["task_id"]
            self.args = d["args"]
            self.kwargs = d["kwargs"]
            self.delay_until = None
            self.retries = 0
            self.max_retries = 0

        def is_expired(self):
            return False

        def to_dict(self):
            return {"id": self.id, "task_id": self.task_id}

    monkeypatch.setattr("asyncmq.jobs.Job.from_dict", lambda d: DummyJob(d))

    backend = DummyBackend()
    await handle_job("q1", raw_job, backend)

    assert imported == [task_id]
    assert backend.calls["update_job_state"][-1][2] == State.COMPLETED
    assert backend.calls["save_job_result"] == [("q1", "j1", "ok")]
    assert backend.calls["ack"] == [("q1", "j1")]


def test_autodiscover_tasks(monkeypatch, tmp_path):
    pkg_dir = tmp_path / "fake_tasks"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")  # make it a package
    (pkg_dir / "a.py").write_text("# empty")  # two modules
    (pkg_dir / "b.py").write_text("# empty")

    monkeypatch.syspath_prepend(str(tmp_path))

    monkeypatch.setattr(
        "asyncmq.conf.settings.tasks",
        ["fake_tasks"],
        raising=True,
    )

    imports = []
    real_import = importlib.import_module

    def spy_import(name, *args, **kwargs):
        imports.append(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", spy_import)

    autodiscover_tasks()

    assert "fake_tasks" in imports
    assert "fake_tasks.a" in imports
    assert "fake_tasks.b" in imports
