from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.conf import monkay
from asyncmq.contrib.dashboard.mixins import DashboardMixin


class DashboardController(DashboardMixin, TemplateController):
    """
    Home page controller: shows total queues, total jobs, and total workers.
    """

    template_name = "index.html"

    async def get(self, request: Request) -> Any:
        backend = monkay.settings.backend

        # 1) get all queues & count them
        queues = await backend.list_queues()
        total_queues = len(queues)

        # 2) count jobs across all states
        total_jobs = 0
        for queue in queues:
            for state in ("waiting", "active", "completed", "failed", "delayed"):
                jobs = await backend.list_jobs(queue, state)
                total_jobs += len(jobs)

        # 3) count registered workers
        workers = await backend.list_workers()
        total_workers = len(workers)

        # 4) Update the context
        context = await super().get_context_data(request)
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
