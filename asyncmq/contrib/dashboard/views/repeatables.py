from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.contrib.dashboard.views.mixins import DashboardMixin

# Dummy data — will later pull from backend
repeatable_jobs = [
    {"task": "send_email", "interval": "5m", "last_run": "2024-05-03 15:00"},
    {"task": "refresh_cache", "cron": "*/10 * * * *", "last_run": "2024-05-03 15:10"},
]


class RepeatablesController(DashboardMixin, TemplateController):
    template_name = "repeatables/repeatables.html"

    async def get(self, request: Request) -> Any:
        queue = request.path_params.get("name", "default")
        context = await super().get_context_data(request)
        context.update(
            {
                "title": f"Repeatables – {queue}",
                "queue": queue,
                "repeatables": repeatable_jobs,
            }
        )
        return await self.render_template(request, context=context)
