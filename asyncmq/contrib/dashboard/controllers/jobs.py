from __future__ import annotations

import datetime as dt
from typing import Any

from lilya.controllers import Controller
from lilya.datastructures import FormData
from lilya.requests import Request
from lilya.responses import JSONResponse, RedirectResponse, Response
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.mixins import DashboardMixin

RawJobData = dict[str, Any]
FormattedJob = dict[str, Any]


class QueueJobController(DashboardMixin, TemplateController):
    """
    Controller for viewing, paginating, and managing jobs within a specific queue.

    Handles job display filtered by state (waiting, active, failed, etc.) and bulk actions
    (retry, remove, cancel) via HTTP POST.
    """

    template_name: str = "jobs/jobs.html"

    def _get_params(self, request: Request) -> tuple[str, int, int]:
        """Extracts and validates state, page, and size parameters."""
        state: str = request.query_params.get("state", "waiting")

        try:
            page: int = int(request.query_params.get("page", 1))
            size: int = int(request.query_params.get("size", 20))
        except ValueError:
            page, size = 1, 20

        return state, page, size

    def _format_job_data(self, raw_job: RawJobData) -> FormattedJob:
        """Formats a single raw job dictionary for template display."""
        # Preference for timestamp fields
        ts: int = raw_job.get("run_at") or raw_job.get("created_at") or 0

        try:
            # Convert Unix timestamp to human-readable format
            created: str = dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            created = "N/A"

        return {
            "id": raw_job.get("id"),
            "status": raw_job.get("status", "n/a"),
            "payload": raw_job,  # Full object for tojson() inspection
            "run_at": raw_job.get("run_at"),  # Original timestamp (optional)
            "created_at": created,  # Formatted string
        }

    async def _fetch_and_paginate_jobs(
        self, queue: str, state: str, page: int, size: int
    ) -> tuple[list[FormattedJob], int, int]:
        """Fetches jobs for the given state, applies pagination, and calculates page counts."""
        backend: Any = monkay.settings.backend

        # Fetch all jobs for the given state
        all_jobs: list[RawJobData] = await backend.list_jobs(queue, state)
        total: int = len(all_jobs)

        # Apply pagination slice
        start: int = (page - 1) * size
        end: int = start + size
        page_jobs: list[RawJobData] = all_jobs[start:end]

        # Format the sliced jobs
        jobs: list[FormattedJob] = [self._format_job_data(raw) for raw in page_jobs]

        total_pages: int = (total + size - 1) // size

        return jobs, total, total_pages

    async def get(self, request: Request) -> Response:
        """
        Handles the GET request, retrieves filtered and paginated jobs, and renders the job list template.
        """
        queue: str = request.path_params.get("name")

        # 1. Parameter Parsing
        state, page, size = self._get_params(request)

        # 2. Fetch and Paginate
        jobs, total, total_pages = await self._fetch_and_paginate_jobs(queue, state, page, size)

        # 3. Build Context and Render
        context: dict[str, Any] = await super().get_context_data(request)
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

    async def post(self, request: Request) -> RedirectResponse:
        """
        Handles bulk actions (retry, remove, cancel) on selected job IDs.
        """
        queue: str = request.path_params.get("name")
        backend: Any = monkay.settings.backend
        form: FormData = await request.form()
        action: str | None = form.get("action")

        # Safely extract job IDs regardless of form submission format
        job_ids: list[str]
        if hasattr(form, "getlist"):
            job_ids = form.getlist("job_id")
        else:
            # Fallback for single value or a comma-delimited string
            raw: str = form.get("job_id") or ""
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

        # Redirect back to the same list/state/page
        state: str = form.get("state", "waiting")
        return RedirectResponse(f"/queues/{queue}/jobs?state={state}", status_code=302)


class JobActionController(Controller):
    """
    Handles single-job actions (retry, remove, cancel) via dedicated AJAX endpoints.
    """

    async def post(self, request: Request, job_id: str, action: str) -> JSONResponse:
        """
        Performs a single action on a job and returns a JSON status response.

        Args:
            request: The incoming Lilya Request object.
            job_id: The ID of the job to act upon (from the path).
            action: The action to perform ('retry', 'remove', or 'cancel') (from the path).

        Returns:
            JSONResponse: Status of the operation ({ok: true} on success).
        """
        queue: str = request.path_params.get("name")
        backend: Any = monkay.settings.backend

        try:
            if action == "retry":
                await backend.retry_job(queue, job_id)
            elif action == "remove":
                await backend.remove_job(queue, job_id)
            elif action == "cancel":
                await backend.cancel_job(queue, job_id)
            else:
                return JSONResponse({"ok": False, "error": "Unknown action"}, status_code=400)
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

        return JSONResponse({"ok": True})
