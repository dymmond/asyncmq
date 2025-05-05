from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.contrib.dashboard.views.mixins import DashboardMixin

# Dummy active workers data
worker_list = [
    {"id": "worker-a1", "queue": "default", "concurrency": 3, "heartbeat": "2024-05-04 11:30"},
    {"id": "worker-b2", "queue": "emails", "concurrency": 2, "heartbeat": "2024-05-04 11:31"},
]


class WorkerController(DashboardMixin, TemplateController):
    template_name = "workers/workers.html"

    async def get(self, request: Request) -> Any:
        context = await super().get_context_data(request)
        context.update(
            {
                "title": "Active Workers",
                "workers": worker_list,
            }
        )
        return await self.render_template(request, context=context)
