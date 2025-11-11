from __future__ import annotations

from typing import Any, Sequence

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
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
        job_states: Sequence[str] = (
            "waiting",
            "active",
            "completed",
            "failed",
            "delayed",
        )

        # 1) Get all queues & count them
        queues: list[str] = await backend.list_queues()
        total_queues: int = len(queues)

        # 2) Count jobs across all states
        total_jobs: int = 0
        for queue in queues:
            for state in job_states:
                # Assuming backend.list_jobs returns a list of job data
                jobs: list[Any] = await backend.list_jobs(queue, state)
                total_jobs += len(jobs)

        # 3) Count registered workers
        workers: list[Any] = await backend.list_workers()
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
