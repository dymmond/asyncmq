from __future__ import annotations

from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.controllers._counts import aggregate_counts
from asyncmq.contrib.dashboard.mixins import DashboardMixin


class DashboardController(DashboardMixin, TemplateController):
    """
    Home page controller for the AsyncMQ dashboard.

    This controller retrieves key metrics across all queues, including the total number
    of queues, the total job count across all states (waiting, active, completed, failed, delayed),
    and the total number of registered workers.
    """

    template_name: str = "index.html"

    async def get(self, request: Request) -> Any:
        """
        Handles the GET request, collects all aggregate metrics from the backend,
        and renders the main dashboard template.

        Args:
            request: The incoming Lilya Request object.

        Returns:
            The rendered HTML response for the dashboard index page.
        """
        backend: Any = monkay.settings.backend

        # 1) Get all queues & count them
        try:
            queues: list[str] = await backend.list_queues()
        except Exception:
            queues = []
        total_queues: int = len(queues)

        # 2) Count jobs across all states
        totals = await aggregate_counts(backend, queues) if queues else {}
        total_jobs: int = sum(totals.values())

        # 3) Count registered workers
        try:
            workers: list[Any] = await backend.list_workers()
        except Exception:
            workers = []
        total_workers: int = len(workers)

        # 4) Update the context
        context: dict[str, Any] = await super().get_context_data(request)
        context.update(
            {
                "title": "Overview",
                "total_queues": total_queues,
                "total_jobs": total_jobs,
                "total_workers": total_workers,
                "active_page": "dashboard",
                "page_header": "Dashboard",
            }
        )

        return await self.render_template(request, context=context)
