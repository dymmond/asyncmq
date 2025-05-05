from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.contrib.dashboard.views.mixins import DashboardMixin

dummy_queues = [
    {"name": "default", "waiting": 12, "active": 3, "completed": 100},
    {"name": "emails", "waiting": 7, "active": 1, "completed": 55},
]


class QueueController(DashboardMixin, TemplateController):
    template_name = "queues/queues.html"

    async def get(self, request: Request) -> Any:
        cc = await super().get_context_data(request)
        cc.update(
            {
                "title": "Queues",
                "queues": dummy_queues,
            }
        )
        return await self.render_template(request, context=cc)
