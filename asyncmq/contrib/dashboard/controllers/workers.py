from __future__ import annotations

import datetime as dt
import math
from types import SimpleNamespace
from typing import Any, Mapping

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.mixins import DashboardMixin

WorkerDisplayInfo = dict[str, Any]


class WorkerController(DashboardMixin, TemplateController):
    """
    Controller for the Workers dashboard page.

    Displays a list of all active workers registered with the backend, including
    their queue assignment, concurrency level, and last recorded heartbeat time.
    Handles user-controlled pagination.
    """

    template_name: str = "workers/workers.html"

    async def get(self, request: Request) -> Any:
        """
        Handles the GET request, retrieves active worker details, applies pagination,
        and renders the workers list page.

        Args:
            request: The incoming Lilya Request object.

        Returns:
            The rendered HTML response for the workers page.
        """
        context: dict[str, Any] = await super().get_context_data(request)

        backend: Any = monkay.settings.backend
        # worker_info is expected to be a list of dictionaries or objects
        worker_info: list[dict[str, Any] | Any] = await backend.list_workers()

        all_workers: list[WorkerDisplayInfo] = []
        for worker in worker_info:
            # Normalize dicts to SimpleNamespace if they aren't already objects
            if isinstance(worker, dict):
                worker = SimpleNamespace(**worker)

            # Format heartbeat timestamp
            hb: dt.datetime = dt.datetime.fromtimestamp(worker.heartbeat)

            all_workers.append(
                {
                    "id": worker.id,
                    "queue": worker.queue,
                    "concurrency": worker.concurrency,
                    "heartbeat": hb.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        # --- Pagination Logic ---
        qs: Mapping[str, Any] = request.query_params

        # Safely parse and clamp page/size to be at least 1
        try:
            page: int = max(1, int(qs.get("page", 1)))
            size: int = max(1, int(qs.get("size", 20)))
        except ValueError:
            page, size = 1, 20

        total: int = len(all_workers)

        # Calculate total pages, defaulting to 1 if total/size is 0
        total_pages: int = math.ceil(total / size) if total > 0 and size else 1

        # Clamp current page to the valid range
        page = min(page, total_pages) if total_pages > 0 else 1

        # Apply slicing
        start: int = (page - 1) * size
        end: int = start + size
        workers: list[WorkerDisplayInfo] = all_workers[start:end]

        # --- Context Update ---
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
                "page_header": "Active Workers",
            }
        )
        return await self.render_template(request, context=context)
