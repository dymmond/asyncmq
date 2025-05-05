from __future__ import annotations

from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views.mixins import DashboardMixin
from asyncmq.core.enums import State


class QueueController(DashboardMixin, TemplateController):
    template_name = "queues/queues.html"

    async def get_queues(self) -> list[str]:
        queue_names = await settings.backend.list_queues()
        queues = []

        for name in queue_names:
            # pull counts for each state
            counts = {}
            for state in (State.WAITING, State.ACTIVE, State.COMPLETED, State.FAILED):
                jobs = await settings.backend.list_jobs(name, state)
                counts[state] = len(jobs)

                queues.append({
                    "name": name,
                    "waiting": counts[State.WAITING],
                    "active": counts[State.ACTIVE],
                    "completed": counts[State.COMPLETED],
                    "failed": counts[State.FAILED],
                })
        return queues

    async def get(self, request: Request) -> Any:
        context = await super().get_context_data(request)
        queues = await self.get_queues()
        context.update(
            {
                "title": "Queues",
                "queues": queues
            }
        )
        return await self.render_template(request, context=context)
