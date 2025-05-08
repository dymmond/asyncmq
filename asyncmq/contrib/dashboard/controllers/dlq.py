import json
from datetime import datetime
from typing import Any

from lilya.requests import Request
from lilya.responses import RedirectResponse
from lilya.templating.controllers import TemplateController

from asyncmq.conf import monkay, settings
from asyncmq.contrib.dashboard.messages import add_message
from asyncmq.contrib.dashboard.mixins import DashboardMixin


class DLQController(DashboardMixin, TemplateController):
    template_name = "dlqs/dlq.html"

    async def get(self, request: Request) -> Any:
        queue = request.path_params["name"]
        backend = monkay.settings.backend

        # pagination params
        try:
            page = int(request.query_params.get("page", 1))
            size = int(request.query_params.get("size", 20))
        except ValueError:
            page, size = 1, 20

        # fetch all failed jobs then slice
        all_jobs = await backend.list_jobs(queue, "failed")
        total = len(all_jobs)
        total_pages = (total + size - 1) // size
        start = (page - 1) * size
        end = start + size
        page_jobs = all_jobs[start:end]

        # format for template
        jobs = []
        for raw in page_jobs:
            # pick a timestamp field
            ts = raw.get("failed_at") or raw.get("timestamp") or raw.get("created_at") or 0
            try:
                created = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                created = "N/A"

            jobs.append(
                {
                    "id": raw.get("id"),
                    "args": json.dumps(raw.get("args", [])),
                    "kwargs": json.dumps(raw.get("kwargs", {})),
                    "created": created,
                }
            )

        # build context
        context = await super().get_context_data(request)
        context.update(
            {
                "page_header": f"DLQ {queue}",
                "queue": queue,
                "jobs": jobs,
                "page": page,
                "size": size,
                "total": total,
                "total_pages": total_pages,
            }
        )

        return await self.render_template(request, context=context)

    async def post(self, request: Request) -> Any:
        queue = request.path_params.get("name")
        backend = monkay.settings.backend
        form = await request.form()
        action = form.get("action")
        page = form.get("page", 1)

        try:
            if hasattr(form, "getall"):
                job_ids = form.getall("job_id")
            else:
                raw = form.get("job_id") or ""
                job_ids = raw.split(",") if "," in raw else [raw]
        except KeyError:
            if action == "remove":
                add_message(request, "error", "You need to select a job to be deleted first.")
            else:
                add_message(request, "info", "You need to select a job to be retried first.")
            return RedirectResponse(
                f"{settings.dashboard_config.dashboard_url_prefix}/queues/{queue}/dlq", status_code=303
            )

        for job_id in job_ids:
            if not job_id:
                continue
            if action == "retry":
                await backend.retry_job(queue, job_id)
            elif action == "remove":
                await backend.remove_job(queue, job_id)

        return RedirectResponse(
            f"{settings.dashboard_config.dashboard_url_prefix}/queues/{queue}/dlq?page={page}", status_code=303
        )
