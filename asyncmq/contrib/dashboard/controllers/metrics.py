from __future__ import annotations

from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
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
        queues: list[str] = await backend.list_queues()

        # Initialize counters for job states
        counts: dict[str, int] = {
            "waiting": 0,
            "active": 0,
            "completed": 0,
            "failed": 0,
            "delayed": 0,
        }

        # 3. Sum up each state across all queues
        for queue in queues:
            for state in counts:
                # Assuming backend.list_jobs returns a list of job data
                jobs: list[Any] = await backend.list_jobs(queue, state)
                counts[state] += len(jobs)

        # 4. Build the metrics payload for the template
        metrics: dict[str, Any] = {
            "throughput": counts["completed"],
            "avg_duration": None,  # TODO: compute from timestamps
            "retries": counts["failed"],
            "failures": counts["failed"],
        }

        # 5. Inject and render
        context.update(
            {
                "title": "Metrics",
                "metrics": metrics,
                "active_page": "metrics",
                "page_header": "System Metrics",
            }
        )
        return await self.render_template(request, context=context)
