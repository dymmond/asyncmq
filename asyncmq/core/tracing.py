from __future__ import annotations

import importlib
from contextlib import contextmanager
from typing import Any, Iterator, Protocol

from asyncmq.core.enums import State


class SpanProtocol(Protocol):
    def set_attribute(self, key: str, value: bool | int | float | str) -> None: ...

    def record_exception(self, exception: BaseException) -> None: ...


class NoopSpan:
    def set_attribute(self, key: str, value: bool | int | float | str) -> None:
        return None

    def record_exception(self, exception: BaseException) -> None:
        return None


NOOP_SPAN = NoopSpan()


def _job_attribute(value: Any) -> bool | int | float | str:
    if value is None:
        return ""
    if isinstance(value, bool | int | float | str):
        return value
    return str(value)


def _job_span_attributes(queue_name: str, job_payload: dict[str, Any]) -> dict[str, bool | int | float | str]:
    return {
        "asyncmq.queue": queue_name,
        "asyncmq.job.id": _job_attribute(job_payload.get("id")),
        "asyncmq.job.task": _job_attribute(job_payload.get("task_id", job_payload.get("task"))),
        "asyncmq.job.status": _job_attribute(job_payload.get("status", State.ACTIVE)),
        "asyncmq.job.retries": _job_attribute(job_payload.get("retries", 0)),
        "asyncmq.job.max_retries": _job_attribute(job_payload.get("max_retries", 0)),
        "asyncmq.job.priority": _job_attribute(job_payload.get("priority", 5)),
    }


@contextmanager
def job_execution_span(
    settings: Any,
    queue_name: str,
    job_payload: dict[str, Any],
) -> Iterator[SpanProtocol]:
    if not bool(getattr(settings, "tracing_enabled", False)):
        yield NOOP_SPAN
        return

    try:
        trace = importlib.import_module("opentelemetry.trace")
    except Exception:
        yield NOOP_SPAN
        return

    tracer_name = str(getattr(settings, "tracing_tracer_name", "asyncmq"))
    span_prefix = str(getattr(settings, "tracing_span_prefix", "asyncmq.job"))
    task_name = str(job_payload.get("task_id", job_payload.get("task", "unknown")))
    attributes = _job_span_attributes(queue_name, job_payload)

    with trace.get_tracer(tracer_name).start_as_current_span(
        f"{span_prefix}.{task_name}",
        attributes=attributes,
    ) as span:
        yield span


def mark_job_span_completed(span: SpanProtocol) -> None:
    span.set_attribute("asyncmq.job.status", State.COMPLETED)


def mark_job_span_exception(span: SpanProtocol, exception: BaseException, *, final_status: str) -> None:
    span.record_exception(exception)
    span.set_attribute("asyncmq.job.status", final_status)
    span.set_attribute("asyncmq.job.error_type", exception.__class__.__name__)
