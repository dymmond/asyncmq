from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.conf import monkay
from asyncmq.contrib.dashboard.mixins import DashboardMixin


class MetricsController(DashboardMixin, TemplateController):
    template_name = "metrics/metrics.html"

    async def get(self, request: Request) -> Any:
        # 1. Base context (title, header, favicon)
        context = await super().get_context_data(request)

        # 2. Fetch all queues
        backend = monkay.settings.backend
        queues: list[str] = await backend.list_queues()

        # initialize counters
        counts = {
            "waiting": 0,
            "active": 0,
            "completed": 0,
            "failed": 0,
            "delayed": 0,
        }

        # sum up each state across all queues
        for queue in queues:
            for state in counts:
                jobs = await backend.list_jobs(queue, state)
                counts[state] += len(jobs)

        # build the metrics payload for the template
        metrics = {
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
