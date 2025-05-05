from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views.mixins import DashboardMixin
from asyncmq.core.enums import State


class MetricsController(DashboardMixin, TemplateController):
    template_name = "metrics/metrics.html"

    async def get(self, request: Request) -> Any:
        # 1. Base context (title, header, favicon)
        context = await super().get_context_data(request)

        # 2. Fetch all queues
        backend = settings.backend
        queue_names: list[str] = await backend.list_queues()
        total_queues: int = len(queue_names)

        # 3. Aggregate job counts across all queues
        total_waiting = total_active = total_completed = total_failed = 0
        for q in queue_names:
            total_waiting += len(await backend.list_jobs(q, State.WAITING))
            total_active += len(await backend.list_jobs(q, State.ACTIVE))
            total_completed += len(await backend.list_jobs(q, State.COMPLETED))
            total_failed += len(await backend.list_jobs(q, State.FAILED))

        # 4. Assemble metrics dict
        metrics: dict[str, Any] = {
            "total_queues": total_queues,
            "waiting": total_waiting,
            "active": total_active,
            "completed": total_completed,
            "failed": total_failed,
        }

        # 5. Inject and render
        context.update(
            {
                "title": "System Metrics",
                "metrics": metrics,
            }
        )
        return await self.render_template(request, context=context)
