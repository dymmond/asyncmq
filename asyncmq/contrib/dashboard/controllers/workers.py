import datetime
from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.conf import monkay
from asyncmq.contrib.dashboard.mixins import DashboardMixin


class WorkerController(DashboardMixin, TemplateController):
    """
    Displays all active workers and their heartbeats.
    """

    template_name = "workers/workers.html"

    async def get(self, request: Request) -> Any:
        # 1. Base context (title, header, favicon)
        context = await super().get_context_data(request)

        backend = monkay.settings.backend
        worker_info = await backend.list_workers()

        # 3. Normalize into simple dicts for Jinja
        workers: list[dict[str, Any]] = []
        for wi in worker_info:
            heartbeat = datetime.datetime.fromtimestamp(wi.heartbeat)
            workers.append(
                {
                    "id": wi.id,
                    "queue": wi.queue,  # or wi.queues if a list
                    "concurrency": wi.concurrency,
                    "heartbeat": heartbeat.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        # 4. Inject and render
        context.update(
            {
                "title": "Active Workers",
                "workers": workers,
                "active_page": "workers",
            }
        )
        return await self.render_template(request, context=context)
