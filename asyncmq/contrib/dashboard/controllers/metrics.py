from __future__ import annotations

from typing import Any

from lilya.controllers import Controller
from lilya.requests import Request
from lilya.responses import JSONResponse
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.controllers._counts import aggregate_counts
from asyncmq.contrib.dashboard.metrics_history import list_metrics_history, record_metrics_snapshot
from asyncmq.contrib.dashboard.mixins import DashboardMixin


class MetricsController(DashboardMixin, TemplateController):
    """
    Controller for the Metrics dashboard page.

    This controller aggregates job statistics across all available queues and job states
    to provide high-level metrics like throughput and failure counts.
    """

    template_name: str = "metrics/metrics.html"

    async def get(self, request: Request) -> Any:
        """
        Handles the GET request, retrieves and aggregates job counts, and renders the metrics dashboard.

        Args:
            request: The incoming Lilya Request object.

        Returns:
            The rendered HTML response for the metrics page.
        """
        # 1. Base context (title, header, favicon)
        context: dict[str, Any] = await super().get_context_data(request)

        # 2. Fetch all queues
        backend: Any = monkay.settings.backend
        try:
            queues: list[str] = await backend.list_queues()
        except Exception:
            queues = []
        total_queues = len(queues)

        counts = (
            await aggregate_counts(backend, queues)
            if queues
            else {"waiting": 0, "active": 0, "completed": 0, "failed": 0, "delayed": 0}
        )
        try:
            total_workers = len(await backend.list_workers())
        except Exception:
            total_workers = 0

        # 4. Build the metrics payload for the template
        metrics: dict[str, Any] = {
            "throughput": counts["completed"],
            "avg_duration": None,  # TODO: compute from timestamps
            "retries": counts["failed"],
            "failures": counts["failed"],
        }
        record_metrics_snapshot(
            metrics=metrics,
            counts=counts,
            total_queues=total_queues,
            total_workers=total_workers,
        )

        # 5. Inject and render
        context.update(
            {
                "title": "Metrics",
                "metrics": metrics,
                "counts": counts,
                "total_queues": total_queues,
                "total_workers": total_workers,
                "metrics_history": list_metrics_history(limit=30),
                "active_page": "metrics",
                "page_header": "System Metrics",
            }
        )
        return await self.render_template(request, context=context)


class MetricsHistoryController(Controller):
    async def get(self, request: Request) -> JSONResponse:
        limit_raw = request.query_params.get("limit", "120")
        try:
            limit = int(limit_raw)
        except ValueError:
            limit = 120
        limit = max(1, min(limit, 500))
        return JSONResponse({"history": list_metrics_history(limit=limit)})
