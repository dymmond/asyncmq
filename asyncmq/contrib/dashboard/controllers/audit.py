from __future__ import annotations

import datetime as dt
import json
from collections import Counter
from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.contrib.dashboard.audit import list_audit_events
from asyncmq.contrib.dashboard.mixins import DashboardMixin

ACTION_OPTIONS: list[str] = [
    "queue.pause",
    "queue.resume",
    "job.retry",
    "job.remove",
    "job.cancel",
    "dlq.retry",
    "dlq.remove",
    "repeatable.create",
    "repeatable.pause",
    "repeatable.resume",
    "repeatable.remove",
]


def format_audit_details(details: dict[str, Any] | None) -> str:
    """Return stable JSON for audit details shown to operators."""
    if not details:
        return ""
    return json.dumps(details, sort_keys=True, indent=2, default=str)


def build_audit_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Summarize the currently visible audit rows for dashboard stat cards."""
    status_counts = Counter(str(row.get("status") or "unknown") for row in rows)
    action_count = len({str(row.get("action") or "") for row in rows if row.get("action")})
    source_count = len({str(row.get("source") or "") for row in rows if row.get("source")})
    return {
        "visible": len(rows),
        "success": status_counts.get("success", 0),
        "failed": status_counts.get("failed", 0),
        "actions": action_count,
        "sources": source_count,
    }


class AuditController(DashboardMixin, TemplateController):
    """Renders the bounded audit trail for dashboard actions shown to operators."""

    template_name = "audit/audit.html"

    async def get(self, request: Request) -> Any:
        """Apply audit filters, normalize timestamps, and render the audit page."""
        action_raw = request.query_params.get("action", "").strip()
        status_raw = request.query_params.get("status", "").strip()
        queue_raw = request.query_params.get("queue", "").strip()
        q_raw = request.query_params.get("q", "").strip()
        limit_raw = request.query_params.get("limit", "200")

        try:
            limit = int(limit_raw)
        except ValueError:
            limit = 200
        limit = max(1, min(limit, 500))

        events = list_audit_events(
            limit=limit,
            action=action_raw or None,
            status=status_raw or None,
            queue=queue_raw or None,
            q=q_raw or None,
        )
        rows: list[dict[str, Any]] = []
        for event in events:
            ts = float(event.get("timestamp", 0.0))
            try:
                event_time = dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError, OSError):
                event_time = "N/A"
            queue = str(event.get("queue") or "")
            job_id = str(event.get("job_id") or "")
            rows.append(
                {
                    **event,
                    "time": event_time,
                    "details_json": format_audit_details(event.get("details")),
                    "target_label": queue or job_id or "-",
                    "target_meta": f"job {job_id}" if queue and job_id else "",
                }
            )

        context: dict[str, Any] = await super().get_context_data(request)
        context.update(
            {
                "title": "Audit Trail",
                "active_page": "audit",
                "page_header": "Queue and Job Action Audit Trail",
                "events": rows,
                "action_options": ACTION_OPTIONS,
                "selected_action": action_raw,
                "selected_status": status_raw,
                "selected_queue": queue_raw,
                "selected_query": q_raw,
                "limit": limit,
                "audit_summary": build_audit_summary(rows),
            }
        )
        return await self.render_template(request, context=context)
