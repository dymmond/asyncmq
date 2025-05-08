from __future__ import annotations

from datetime import datetime
from typing import Any

from lilya.controllers import Controller
from lilya.requests import Request
from lilya.responses import JSONResponse, RedirectResponse
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
        for raw in page_jobs:
            # raw is the dict you originally saved via enqueue, containing at least
            # id, status, args, kwargs, and maybe timestamp/run_at/created_at
            ts = raw.get("run_at") or raw.get("created_at") or 0
            try:
                created = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                created = "N/A"

            jobs.append(
                {
                    "id": raw.get("id"),
                    "status": raw.get("status", "n/a"),
                    "payload": raw,  # full object for tojson()
                    "run_at": raw.get("run_at"),  # could be None
                    "created_at": created,  # formatted string
                }
            )

        total_pages = (total + size - 1) // size
        state = request.query_params.get("state", "waiting")

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
                "state": state,
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


class JobActionController(Controller):
    """
    Handles single‐job actions via AJAX:
      POST /queues/{name}/jobs/{job_id}/{action}
      where action ∈ {'retry','remove','cancel'}
    Returns JSON { ok: true } on success or { ok: false, error: '...' }.
    """

    async def post(self, request: Request, job_id: str | int, action: str) -> Any:
        queue = request.path_params.get("name")
        backend = monkay.settings.backend

        try:
            if action == "retry":
                await backend.retry_job(queue, job_id)  # type: ignore
            elif action == "remove":
                await backend.remove_job(queue, job_id)  # type: ignore
            elif action == "cancel":
                await backend.cancel_job(queue, job_id)  # type: ignore
            else:
                return JSONResponse({"ok": False, "error": "Unknown action"}, status_code=400)
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

        return JSONResponse({"ok": True})
