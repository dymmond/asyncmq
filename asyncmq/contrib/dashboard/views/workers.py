from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views.mixins import DashboardMixin

# Dummy active workers data
worker_list = [
    {"id": "worker-a1", "queue": "default", "concurrency": 3, "heartbeat": "2024-05-04 11:30"},
    {"id": "worker-b2", "queue": "emails", "concurrency": 2, "heartbeat": "2024-05-04 11:31"},
]


class WorkerController(DashboardMixin, TemplateController):
    """
    Displays all active workers and their heartbeats.
    """
    template_name = "workers/workers.html"

    async def get(self, request: Request) -> Any:
        # 1. Base context (title, header, favicon)
        context = await super().get_context_data(request)

        # 2. Fetch worker info from backend
        #    (You'll need to implement `list_workers()` on your backend to return
        #     a list of worker‐info objects or dicts with id, queue(s), concurrency,
        #     and last heartbeat timestamp.)
        backend = settings.backend
        worker_infos = await backend.list_workers()

        # 3. Normalize into simple dicts for Jinja
        workers: list[dict[str, Any]] = []
        for wi in worker_infos:
            workers.append({
                "id":          wi.id,
                "queue":       wi.queue,              # or wi.queues if a list
                "concurrency": wi.concurrency,
                "heartbeat":   wi.heartbeat.strftime("%Y-%m-%d %H:%M:%S"),
            })

        # 4. Inject and render
        context.update({
            "title":   "Active Workers",
            "workers": workers,
        })
        return await self.render_template(request, context=context)
