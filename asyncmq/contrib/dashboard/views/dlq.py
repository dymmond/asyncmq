import json
from datetime import datetime
from typing import Any

from lilya.requests import Request
from lilya.responses import RedirectResponse
from lilya.templating.controllers import TemplateController

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views.mixins import DashboardMixin


class DLQController(DashboardMixin, TemplateController):
    template_name = "dlqs/dlq.html"

    async def get(self, request: Request) -> Any:
        queue = request.path_params.get("name")
        backend = settings.backend

        failed = await backend.list_jobs(queue, "failed")
        jobs = []

        for job in failed:
            ts = job.get("timestamp") or job.get("created_at") or 0
            try:
                created = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                created = "N/A"

            jobs.append(
                {
                    "id": job.get("id"),
                    "args": json.dumps(job.get("args", [])),
                    "kwargs": json.dumps(job.get("kwargs", {})),
                    "created": created,
                }
            )

        context = await super().get_context_data(request)
        context.update(
            {
                "title": f"Dead Letter Queue – {queue}",
                "queue": queue,
                "jobs": jobs,
            }
        )

        return await self.render_template(request, context=context)

    async def post(self, request: Request) -> Any:
        queue = request.path_params.get("name")
        backend = settings.backend
        form = await request.form()
        action = form.get("action")

        if hasattr(form, "getlist"):
            job_ids = form.getlist("job_id")
        else:
            raw = form.get("job_id") or ""
            job_ids = raw.split(",") if "," in raw else [raw]

        for job_id in job_ids:
            if not job_id:
                continue
            if action == "retry":
                await backend.retry_job(queue, job_id)
            elif action == "remove":
                await backend.remove_job(queue, job_id)

        return RedirectResponse(f"/queues/{queue}/dlq")
