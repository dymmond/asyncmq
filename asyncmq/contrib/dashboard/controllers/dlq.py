from __future__ import annotations

import datetime as dt
import json
from typing import Any

from lilya.datastructures import URL, FormData
from lilya.requests import Request
from lilya.responses import RedirectResponse, Response
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.messages import add_message
from asyncmq.contrib.dashboard.mixins import DashboardMixin

RawJobData = dict[str, Any]
FormattedJob = dict[str, Any]


class DLQController(DashboardMixin, TemplateController):
    """
    Controller for viewing and managing jobs in a Dead Letter Queue (DLQ).

    Handles pagination, display formatting, and bulk actions (retry/remove) for failed jobs
    in a specific queue.
    """

    template_name: str = "dlqs/dlq.html"

    def get_return_url(self, request: Request, **params: Any) -> URL:
        """Calculates the URL path for redirecting back to this controller."""
        return request.url_path_for("dlq", **params)

    def _get_pagination_params(self, request: Request) -> tuple[int, int]:
        """Extracts and validates pagination parameters (page and size)."""
        try:
            page: int = int(request.query_params.get("page", 1))
            size: int = int(request.query_params.get("size", 20))
        except ValueError:
            page, size = 1, 20
        return page, size

    def _format_job_timestamp(self, raw_job: RawJobData) -> str:
        """Extracts the timestamp from a raw job and formats it."""
        # Preference: failed_at > timestamp > created_at
        ts: int = raw_job.get("failed_at") or raw_job.get("timestamp") or raw_job.get("created_at") or 0

        try:
            # Convert Unix timestamp to human-readable format
            created: str = dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            created = "N/A"
        return created

    async def _fetch_and_format_jobs(self, queue: str, page: int, size: int) -> tuple[list[FormattedJob], int, int]:
        """
        Fetches all failed jobs for the queue, handles slicing, and formats them for the template.

        Returns: (list of formatted jobs, Total job count, Total page count)
        """
        backend = monkay.settings.backend

        # Fetch all failed jobs (Backend must support this)
        all_jobs: list[RawJobData] = await backend.list_jobs(queue, "failed")
        total: int = len(all_jobs)
        total_pages: int = (total + size - 1) // size

        # Apply pagination slice
        start: int = (page - 1) * size
        end: int = start + size
        page_jobs: list[RawJobData] = all_jobs[start:end]

        formatted_jobs: list[FormattedJob] = []
        for raw in page_jobs:
            created_at: str = self._format_job_timestamp(raw)
            formatted_jobs.append(
                {
                    "id": raw.get("id"),
                    "args": json.dumps(raw.get("args", [])),
                    "kwargs": json.dumps(raw.get("kwargs", {})),
                    "created": created_at,
                }
            )

        return formatted_jobs, total, total_pages

    async def _build_context(
        self,
        request: Request,
        queue: str,
        jobs: list[FormattedJob],
        page: int,
        size: int,
        total: int,
        total_pages: int,
    ) -> dict[str, Any]:
        """Assembles the final context dictionary for the template renderer."""
        context: dict[str, Any] = await super().get_context_data(request)
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
        return context

    async def get(self, request: Request) -> Response:
        """
        Handles displaying the Dead Letter Queue, including pagination.
        """
        queue: str = request.path_params["name"]

        # 1. Get pagination parameters
        page, size = self._get_pagination_params(request)

        # 2. Fetch and format jobs
        jobs, total, total_pages = await self._fetch_and_format_jobs(queue, page, size)

        # 3. Build context and render
        context = await self._build_context(request, queue, jobs, page, size, total, total_pages)

        return await self.render_template(request, context=context)

    async def post(self, request: Request) -> RedirectResponse:
        """
        Handles actions (retry or remove) on selected job IDs in the DLQ.
        """
        queue: str | None = request.path_params.get("name")
        backend = monkay.settings.backend
        form: FormData = await request.form()
        action: str | None = form.get("action")

        # Page is retrieved to redirect the user back to the correct page after the action
        page: int = int(form.get("page", 1))

        try:
            # 1. Safely extract job IDs regardless of single/multi-select form structure
            job_ids: list[str]
            if hasattr(form, "getall"):
                # Standard for multiple same-name inputs
                job_ids = form.getall("job_id")
            else:
                # Fallback for simple forms/single inputs
                raw: str = form.get("job_id") or ""
                job_ids = raw.split(",") if "," in raw else [raw]

            # Ensure list contains only non-empty strings
            job_ids = [job_id for job_id in job_ids if job_id]

            if not job_ids:
                raise KeyError  # Trigger message for no selection

        except KeyError:
            # 2. Handle case where no job IDs were selected
            if action == "remove":
                add_message(request, "error", "You need to select a job to be deleted first.")
            else:
                add_message(request, "info", "You need to select a job to be retried first.")
            return RedirectResponse(self.get_return_url(request, name=queue), status_code=303)

        # 3. Process actions for selected IDs
        for job_id in job_ids:
            if action == "retry":
                await backend.retry_job(queue, job_id)
            elif action == "remove":
                await backend.remove_job(queue, job_id)

        # 4. Redirect back to the original page
        return RedirectResponse(f"{self.get_return_url(request, name=queue)}?page={page}", status_code=303)
