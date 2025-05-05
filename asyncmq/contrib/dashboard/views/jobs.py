from __future__ import annotations

from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views.mixins import DashboardMixin
from asyncmq.core.enums import State


class QueueJobController(DashboardMixin, TemplateController):
    template_name = "jobs/jobs.html"

    async def get_jobs(self, queue_name: str, state: str) -> list[dict]:
        backend = settings.backend
        jobs: list[dict[str, Any]] = await backend.list_jobs(queue_name, state)
        return jobs


    async def get(self, request: Request) -> Any:
        queue_name = request.path_params.get("name", "default")
        state: State = State(request.query_params.get("state", State.WAITING))

        jobs = await self.get_jobs(queue_name, state)
        context = await super().get_context_data(request)
        context.update(
            {
                "title": f"Jobs in '{queue_name}'",
                "queue": queue_name,
                "jobs": jobs,
            }
        )
        return await self.render_template(request, context=context)
