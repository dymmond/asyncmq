from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.contrib.dashboard.views.mixins import DashboardMixin


class DashboardController(DashboardMixin, TemplateController):
    template_name = "index.html"

    async def get(self, request: Request) -> Any:
        context = await super().get_context_data(request)
        context.update(
            {
                "title": "Dashboard",
            }
        )
        return await self.render_template(request)
