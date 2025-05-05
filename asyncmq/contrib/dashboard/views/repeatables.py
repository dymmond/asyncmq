from __future__ import annotations

from datetime import datetime
from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views.mixins import DashboardMixin


class RepeatablesController(DashboardMixin, TemplateController):
    template_name = "repeatables/repeatables.html"

    async def get_repeatables(self, queue_name: str) -> list[dict[str, Any]]:
        backend = settings.backend
        repeatables = await backend.list_repeatables(queue_name)

        rows: list[dict[str, Any]] = []
        for repeatable in repeatables:
            jd = repeatable.job_def
            task_id = jd.get("task_id") or jd.get("name")
            every = jd.get("every")
            cron = jd.get("cron")

            # human‐friendly next‐run timestamp
            try:
                next_run = datetime.fromtimestamp(repeatable.next_run).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except Exception:
                next_run = "—"

            rows.append({
                "task_id": task_id,
                "every": every,
                "cron": cron,
                "next_run": next_run,
                "paused": repeatable.paused,
            })

        return rows

    async def get(self, request: Request) -> Any:
        queue = request.path_params.get("name")
        repeatables = await self.get_repeatables(queue)

        context = await super().get_context_data(request)
        context.update(
            {
                "title": f"Repeatables – {queue}",
                "queue": queue,
                "repeatables": repeatables,
            }
        )

        # 5. Render
        return await self.render_template(request, context=context)
