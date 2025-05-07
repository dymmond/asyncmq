from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from lilya.requests import Request
from lilya.responses import RedirectResponse
from lilya.templating.controllers import TemplateController

from asyncmq.conf import monkay
from asyncmq.contrib.dashboard.mixins import DashboardMixin


class QueueJobController(DashboardMixin, TemplateController):
    template_name = "jobs/jobs.html"

    async def get(self, request: Request) -> Any:
        queue = request.path_params.get("name")
        backend = monkay.settings.backend

        # filters & pagination
        state = request.query_params.get("state", "waiting")
        try:
            page = int(request.query_params.get("page", 1))
            size = int(request.query_params.get("size", 20))
        except ValueError:
            page, size = 1, 20

        # fetch & slice
        all_jobs = await backend.list_jobs(queue, state)
        total = len(all_jobs)
        start = (page - 1) * size
        end = start + size
        page_jobs = all_jobs[start:end]

        # format each job
        jobs = []
        for job in page_jobs:
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
                    "page_header": f"Jobs in '{queue}' ({state})",
                }
            )

        total_pages = (total + size - 1) // size

        context = await super().get_context_data(request)
        context.update(
            {
                "title": f"Jobs in '{queue}'",
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

        if hasattr(form, "getlist"):
            job_ids = form.getlist("job_id")
        else:
            # fallback if single value or a comma-delimited string
            raw = form.get("job_id") or ""
            job_ids = raw.split(",") if "," in raw else [raw]

        for job_id in job_ids:
            if not job_id:
                continue
            if action == "retry":
                await backend.retry_job(queue, job_id)
            elif action == "remove":
                await backend.remove_job(queue, job_id)
            elif action == "cancel":
                await backend.cancel_job(queue, job_id)

            # Redirect back to this page, preserving state & pagination
        state = form.get("state", "waiting")

        # back to the same list/state
        return RedirectResponse(f"/queues/{queue}/jobs?state={state}")
