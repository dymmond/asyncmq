from typing import Any

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq.contrib.dashboard.views.mixins import DashboardMixin

# Dummy data for jobs in a queue
dummy_jobs = {
    "default": [
        {"id": "job1", "status": "waiting", "created_at": "2024-05-04 10:00", "args": ["arg1"], "result": None},
        {"id": "job2", "status": "active", "created_at": "2024-05-04 10:01", "args": ["arg2"], "result": None},
        {"id": "job3", "status": "completed", "created_at": "2024-05-04 10:02", "args": ["arg3"], "result": "ok"},
    ]
}


class QueueJobController(DashboardMixin, TemplateController):
    template_name = "jobs/jobs.html"

    async def get(self, request: Request) -> Any:
        queue_name = request.path_params.get("name", "default")
        jobs = dummy_jobs.get(queue_name, [])
        context = await super().get_context_data(request)
        context.update(
            {
                "title": f"Jobs in '{queue_name}'",
                "queue": "default",
                "jobs": jobs,
            }
        )
        return await self.render_template(request, context=context)
