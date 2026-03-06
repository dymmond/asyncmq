from __future__ import annotations

import datetime as dt
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


class AuditController(DashboardMixin, TemplateController):
    template_name = "audit/audit.html"

    async def get(self, request: Request) -> Any:
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
            rows.append({**event, "time": event_time})

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
            }
        )
        return await self.render_template(request, context=context)
