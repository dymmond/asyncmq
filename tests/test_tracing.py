from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from typing import Iterator

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.jobs import Job
from asyncmq.tasks import TASK_REGISTRY
from asyncmq.workers import handle_job

pytestmark = pytest.mark.anyio


class FakeSpan:
    def __init__(self, name: str, attributes: dict[str, bool | int | float | str]) -> None:
        self.name = name
        self.attributes = dict(attributes)
        self.exceptions: list[BaseException] = []

    def set_attribute(self, key: str, value: bool | int | float | str) -> None:
        self.attributes[key] = value

    def record_exception(self, exception: BaseException) -> None:
        self.exceptions.append(exception)


class FakeTracer:
    def __init__(self, spans: list[FakeSpan]) -> None:
        self.spans = spans

    @contextmanager
    def start_as_current_span(
        self,
        name: str,
        attributes: dict[str, bool | int | float | str],
    ) -> Iterator[FakeSpan]:
        span = FakeSpan(name, attributes)
        self.spans.append(span)
        yield span


class FakeTrace:
    def __init__(self, spans: list[FakeSpan], tracer_names: list[str]) -> None:
        self.spans = spans
        self.tracer_names = tracer_names

    def get_tracer(self, name: str) -> FakeTracer:
        self.tracer_names.append(name)
        return FakeTracer(self.spans)


def _install_fake_opentelemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[FakeSpan], list[str]]:
    spans: list[FakeSpan] = []
    tracer_names: list[str] = []
    otel = types.ModuleType("opentelemetry")
    trace = types.ModuleType("opentelemetry.trace")
    trace.get_tracer = FakeTrace(spans, tracer_names).get_tracer  # type: ignore[attr-defined]
    otel.trace = trace  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "opentelemetry", otel)
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", trace)
    return spans, tracer_names


async def test_worker_job_tracing_records_completed_span(monkeypatch: pytest.MonkeyPatch) -> None:
    spans, tracer_names = _install_fake_opentelemetry(monkeypatch)
    monkeypatch.setattr(settings, "tracing_enabled", True)
    monkeypatch.setattr(settings, "tracing_tracer_name", "tests.tracer")
    monkeypatch.setattr(settings, "tracing_span_prefix", "tests.job")

    async def traced_task(value: str) -> str:
        return value

    TASK_REGISTRY["tests.traced_task"] = {"func": traced_task}
    backend = InMemoryBackend()
    queue = "tracing"
    job = Job(task_id="tests.traced_task", args=["ok"], kwargs={}, job_id="traced-ok")
    await backend.enqueue(queue, job.to_dict())
    raw = await backend.dequeue(queue)

    assert raw is not None
    await handle_job(queue, raw, backend)

    assert await backend.get_job_state(queue, job.id) == State.COMPLETED
    assert tracer_names == ["tests.tracer"]
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "tests.job.tests.traced_task"
    assert span.attributes["asyncmq.queue"] == queue
    assert span.attributes["asyncmq.job.id"] == job.id
    assert span.attributes["asyncmq.job.task"] == job.task_id
    assert span.attributes["asyncmq.job.status"] == State.COMPLETED
    assert span.exceptions == []


async def test_worker_job_tracing_records_execution_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    spans, _ = _install_fake_opentelemetry(monkeypatch)
    monkeypatch.setattr(settings, "tracing_enabled", True)
    monkeypatch.setattr(settings, "tracing_span_prefix", "tests.job")

    async def failing_task() -> None:
        raise ValueError("boom")

    TASK_REGISTRY["tests.failing_traced_task"] = {"func": failing_task}
    backend = InMemoryBackend()
    queue = "tracing"
    job = Job(task_id="tests.failing_traced_task", args=[], kwargs={}, job_id="traced-fail", max_retries=0)
    await backend.enqueue(queue, job.to_dict())
    raw = await backend.dequeue(queue)

    assert raw is not None
    await handle_job(queue, raw, backend)

    assert await backend.get_job_state(queue, job.id) == State.FAILED
    assert len(spans) == 1
    span = spans[0]
    assert span.attributes["asyncmq.job.status"] == "error"
    assert span.attributes["asyncmq.job.error_type"] == "ValueError"
    assert len(span.exceptions) == 1
    assert str(span.exceptions[0]) == "boom"
