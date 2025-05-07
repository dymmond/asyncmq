from __future__ import annotations

from typing import Any

from lilya.requests import Request
from lilya.responses import RedirectResponse
from lilya.templating.controllers import TemplateController

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.mixins import DashboardMixin


class QueueController(DashboardMixin, TemplateController):
    template_name = "queues/queues.html"

    async def get_queues(self) -> list[dict[str, Any]]:
        queues = await settings.backend.list_queues()

        rows = []
        for q in queues:
            # paused state (some backends may not support it)
            paused = False
            if hasattr(settings.backend, "is_queue_paused"):
                paused = await settings.backend.is_queue_paused(q)

            # counts by state
            counts = {}
            for state in ("waiting", "active", "delayed", "failed", "completed"):
                jobs = await settings.backend.list_jobs(q, state)
                counts[state] = len(jobs)

            rows.append(
                {
                    "name": q,
                    "paused": paused,
                    "waiting": counts["waiting"],
                    "active": counts["active"],
                    "delayed": counts["delayed"],
                    "failed": counts["failed"],
                    "completed": counts["completed"],
                }
            )
        return rows

    async def get(self, request: Request) -> Any:
        context = await super().get_context_data(request)
        queues = await self.get_queues()
        context.update(
            {
                "title": "Queues",
                "queues": queues,
                "active_page": "queues",
            }
        )
        return await self.render_template(request, context=context)


class QueueDetailController(DashboardMixin, TemplateController):
    """
    Shows detailed info for a single queue, and allows pause/resume.
    """

    template_name = "queues/info.html"

    async def get(self, request: Request) -> Any:
        backend = settings.backend
        q = request.path_params["name"]

        # get paused state
        paused = False
        if hasattr(backend, "is_queue_paused"):
            paused = await backend.is_queue_paused(q)

        # counts by state
        counts = {}
        for state in ("waiting", "active", "delayed", "failed", "completed"):
            jobs = await backend.list_jobs(q, state)
            counts[state] = len(jobs)

        context = await super().get_context_data(request)
        context.update(
            {
                "title": f"Queue '{q}'",
                "paused": paused,
                "counts": counts,
                "active_page": "queues",
            }
        )

        return await self.render_template(request, context=context)

    async def post(self, request: Request) -> Any:
        """
        Handles form POSTs from the pause/resume buttons.
        """
        backend = settings.backend
        q = request.path_params["name"]
        action = (await request.form()).get("action")

        if action == "pause" and hasattr(backend, "pause_queue"):
            await backend.pause_queue(q)
        elif action == "resume" and hasattr(backend, "resume_queue"):
            await backend.resume_queue(q)

        return RedirectResponse(f"/queues/{q}")
