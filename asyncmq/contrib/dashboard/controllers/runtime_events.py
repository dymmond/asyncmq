from __future__ import annotations

import datetime as dt
from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.contrib.dashboard.mixins import DashboardMixin
from asyncmq.contrib.dashboard.redaction import redact_for_display, to_pretty_json
from asyncmq.core.event import event_emitter

EVENT_OPTIONS: list[str] = [
    "job:started",
    "job:completed",
    "job:failed",
    "job:cancelled",
    "job:expired",
    "job:ready",
    "job:duplicated",
    "job:deduplicated",
    "job:debounced",
]
FAILURE_EVENT_PARTS: tuple[str, ...] = ("failed", "error", "expired", "cancelled")


def format_event_time(value: Any) -> str:
    """Format a runtime event timestamp for operator display."""
    try:
        return dt.datetime.fromtimestamp(float(value)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return "N/A"


def event_data_value(data: Any, *keys: str) -> str:
    """Extract a string value from generic runtime event data."""
    if not isinstance(data, dict):
        return ""
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def format_runtime_event(record: dict[str, Any]) -> dict[str, Any]:
    """Normalize one runtime event history record for the Events page."""
    event_name = str(record.get("event") or "unknown")
    data = record.get("data")
    queue = event_data_value(data, "queue", "queue_name")
    job_id = event_data_value(data, "job_id", "id")
    task_id = event_data_value(data, "task_id", "task", "name")
    state = event_data_value(data, "status", "state") or event_name.rsplit(":", 1)[-1]
    return {
        "time": format_event_time(record.get("timestamp")),
        "event": event_name,
        "queue": queue or "-",
        "job_id": job_id or "-",
        "task_id": task_id or "-",
        "state": state,
        "data_json": to_pretty_json(redact_for_display(data)),
        "is_failure": any(part in event_name.lower() for part in FAILURE_EVENT_PARTS),
    }


def build_event_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Summarize visible runtime event rows for dashboard stat cards."""
    queues = {row["queue"] for row in rows if row["queue"] != "-"}
    event_types = {row["event"] for row in rows}
    failures = sum(1 for row in rows if row["is_failure"])
    return {
        "visible": len(rows),
        "event_types": len(event_types),
        "queues": len(queues),
        "failures": failures,
    }


class RuntimeEventController(DashboardMixin, TemplateController):
    """Renders bounded local runtime events emitted by AsyncMQ."""

    template_name = "events/history.html"

    async def get(self, request: Request) -> Any:
        """Apply runtime event filters and render the Events page."""
        event_raw = request.query_params.get("event", "").strip()
        queue_raw = request.query_params.get("queue", "").strip()
        q_raw = request.query_params.get("q", "").strip()
        limit_raw = request.query_params.get("limit", "200")

        try:
            limit = int(limit_raw)
        except ValueError:
            limit = 200
        limit = max(1, min(limit, 500))

        records = event_emitter.list_history(
            limit=limit,
            event=event_raw or None,
            queue=queue_raw or None,
            q=q_raw or None,
        )
        rows = [format_runtime_event(record) for record in records]

        context: dict[str, Any] = await super().get_context_data(request)
        context.update(
            {
                "title": "Events",
                "active_page": "events",
                "page_header": "Runtime Events",
                "events": rows,
                "event_options": EVENT_OPTIONS,
                "selected_event": event_raw,
                "selected_queue": queue_raw,
                "selected_query": q_raw,
                "limit": limit,
                "event_summary": build_event_summary(rows),
            }
        )
        return await self.render_template(request, context=context)
