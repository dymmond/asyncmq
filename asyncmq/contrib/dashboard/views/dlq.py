from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views.mixins import DashboardMixin
from asyncmq.core.enums import State


class DQLController(DashboardMixin, TemplateController):
    template_name = "dlqs/dlq.html"

    async def get_jobs(self, queue_name: str) -> list[dict]:
        jobs: list[dict[str, Any]] = await settings.backend.list_jobs(queue_name, State.FAILED)
        return jobs

    async def get(self, request: Request) -> Any:
        queue = request.path_params.get("name", "default")
        context = await super().get_context_data(request)

        jobs = await self.get_jobs(queue)
        context.update(
            {
                "title": f"Dead Letter Queue – {queue}",
                "queue": queue,
                "jobs": jobs,
            }
        )

        return await self.render_template(request, context=context)
