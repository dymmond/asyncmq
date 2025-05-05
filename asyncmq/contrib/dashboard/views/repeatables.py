from __future__ import annotations

from datetime import datetime
from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.backends.base import RepeatableInfo
from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views.mixins import DashboardMixin


class RepeatablesController(DashboardMixin, TemplateController):
    template_name = "repeatables/repeatables.html"

    async def get_repeatables(self, queue_name: str) -> list[dict[str, Any]]:
        backend = settings.backend
        infos: list[RepeatableInfo] = await backend.list_repeatables(queue_name)
        repeatables: list[dict[str, Any]] = []

        for info in infos:
            jd = info.job_def
            ts = datetime.fromtimestamp(info.next_run)
            repeatables.append({
                "task": jd.get("task") or jd.get("name"),
                "cron": jd.get("cron"),
                "interval": jd.get("interval"),
                "last_run": ts.strftime("%Y-%m-%d %H:%M:%S"),
            })

        return repeatables


    async def get(self, request: Request) -> Any:
        queue = request.path_params.get("name")
        repeatables = await self.get_repeatables(queue)

        context = await super().get_context_data(request)
        context.update({
            "title":       f"Repeatables – {queue}",
            "queue":       queue,
            "repeatables": repeatables,
        })

        # 5. Render
        return await self.render_template(request, context=context)
