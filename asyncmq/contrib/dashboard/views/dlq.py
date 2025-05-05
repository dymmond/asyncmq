from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.contrib.dashboard.views.mixins import DashboardMixin

# Dummy DLQ jobs
dlq_jobs = [
    {"id": "job404", "error": "TimeoutError", "created_at": "2024-05-03 14:30"},
    {"id": "job408", "error": "ConnectionRefused", "created_at": "2024-05-03 14:35"},
]


class DQLController(DashboardMixin, TemplateController):
    template_name = "dlqs/dlq.html"

    async def get(self, request: Request) -> Any:
        queue = request.path_params.get("name", "default")
        context = await super().get_context_data(request)
        context.update(
            {
                "title": f"Dead Letter Queue – {queue}",
                "queue": queue,
                "jobs": dlq_jobs,
            }
        )

        return await self.render_template(request, context=context)
