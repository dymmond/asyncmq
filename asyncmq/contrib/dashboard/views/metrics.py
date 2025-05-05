from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.contrib.dashboard.views.mixins import DashboardMixin

# Dummy data simulating metrics
metrics_data = {"throughput": "42 jobs/min", "avg_duration": "230ms", "retries": 12, "failures": 5}


class MetricsController(DashboardMixin, TemplateController):
    template_name = "metrics/metrics.html"

    async def get(self, request: Request) -> Any:
        context = await super().get_context_data(request)
        context.update(
            {
                "title": "System Metrics",
                "metrics": metrics_data,
            }
        )
        return await self.render_template(request, context=context)
