import datetime
import math
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
        context = await super().get_context_data(request)

        backend = monkay.settings.backend
        worker_info = await backend.list_workers()

        all_workers: list[dict[str, Any]] = []
        for wi in worker_info:
            hb = datetime.datetime.fromtimestamp(wi.heartbeat)
            all_workers.append(
                {
                    "id": wi.id,
                    "queue": wi.queue,
                    "concurrency": wi.concurrency,
                    "heartbeat": hb.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        qs = request.query_params
        page = max(1, int(qs.get("page", 1)))
        size = max(1, int(qs.get("size", 20)))

        total = len(all_workers)
        total_pages = math.ceil(total / size) if size else 1
        page = min(page, total_pages)

        start = (page - 1) * size
        end = start + size
        workers = all_workers[start:end]

        context.update(
            {
                "title": "Active Workers",
                "workers": workers,
                "active_page": "workers",
                "page": page,
                "size": size,
                "total": total,
                "total_pages": total_pages,
                "page_sizes": [10, 20, 50, 100],
            }
        )
        return await self.render_template(request, context=context)
