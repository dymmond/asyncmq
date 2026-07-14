from __future__ import annotations

import datetime as dt
from typing import Any

from lilya.datastructures import FormData
from lilya.requests import Request
from lilya.responses import RedirectResponse, Response
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.audit import record_audit_event
from asyncmq.contrib.dashboard.messages import add_message
from asyncmq.contrib.dashboard.mixins import DashboardMixin
from asyncmq.contrib.dashboard.redaction import to_pretty_json
from asyncmq.contrib.dashboard.urls import dashboard_path_for
from asyncmq.core.inspection import JobInspectionPage, extract_job_task, inspect_job_page

RawJobData = dict[str, Any]
FormattedJob = dict[str, Any]
DLQ_PAGE_SIZES: tuple[int, ...] = (20, 50, 100)


class DLQController(DashboardMixin, TemplateController):
    """
    Controller for viewing and managing jobs in a Dead Letter Queue (DLQ).

    Handles pagination, display formatting, and bulk actions (retry/remove) for failed jobs
    in a specific queue.
    """

    template_name: str = "dlqs/dlq.html"

    def get_return_url(self, request: Request, **params: Any) -> str:
        """Calculates the URL path for redirecting back to this controller."""
        return dashboard_path_for(request, "dlq", **params)

    def _get_pagination_params(self, request: Request) -> tuple[int, int]:
        """Extracts and validates pagination parameters (page and size)."""
        try:
            page: int = max(1, int(request.query_params.get("page", 1)))
        except ValueError:
            page = 1
        try:
            size: int = int(request.query_params.get("size", 20))
        except ValueError:
            size = 20
        if size not in DLQ_PAGE_SIZES:
            size = 20
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

    def _format_job_data(self, raw_job: RawJobData) -> FormattedJob:
        """Format one runtime-owned failed job for safe operator display."""
        return {
            "id": raw_job.get("id") or raw_job.get("job_id") or "n/a",
            "task_id": extract_job_task(raw_job) or "n/a",
            "error": raw_job.get("last_error") or raw_job.get("error") or raw_job.get("failure_reason") or "",
            "args_json": to_pretty_json(raw_job.get("args", [])),
            "kwargs_json": to_pretty_json(raw_job.get("kwargs", {})),
            "payload_json": to_pretty_json(raw_job),
            "created": self._format_job_timestamp(raw_job),
        }

    async def _fetch_and_format_jobs(
        self,
        queue: str,
        page: int,
        size: int,
    ) -> tuple[list[FormattedJob], int, int, int]:
        """
        Fetch failed jobs through the backend inspection contract and format them.

        Returns: (formatted jobs, total job count, total page count, current page).
        """
        backend = monkay.settings.backend

        inspect_jobs = getattr(backend, "inspect_jobs", None)
        if callable(inspect_jobs):
            try:
                inspected: JobInspectionPage = await inspect_jobs(
                    queue,
                    "failed",
                    page=page,
                    size=size,
                    sort="newest",
                )
            except Exception:
                inspected = JobInspectionPage(jobs=[], total=0, page=1, size=size, total_pages=1)
        else:
            try:
                all_jobs: list[RawJobData] = await backend.list_jobs(queue, "failed")
            except Exception:
                all_jobs = []
            inspected = inspect_job_page(
                all_jobs,
                page=page,
                size=size,
                sort="newest",
            )

        page_jobs = list(inspected.jobs)
        total = int(inspected.total)
        total_pages = max(1, int(inspected.total_pages))
        page = min(max(1, int(inspected.page)), total_pages)
        formatted_jobs = [self._format_job_data(raw) for raw in page_jobs]

        return formatted_jobs, total, total_pages, page

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
                "title": f"DLQ {queue}",
                "page_header": f"DLQ {queue}",
                "queue": queue,
                "jobs": jobs,
                "page": page,
                "size": size,
                "total": total,
                "total_pages": total_pages,
                "page_sizes": DLQ_PAGE_SIZES,
                "active_page": "queues",
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
        jobs, total, total_pages, page = await self._fetch_and_format_jobs(queue, page, size)

        # 3. Build context and render
        context = await self._build_context(request, queue, jobs, page, size, total, total_pages)

        return await self.render_template(request, context=context)

    async def post(self, request: Request) -> RedirectResponse:
        """
        Handles actions (retry or remove) on selected job IDs in the DLQ.
        """
        queue: str = request.path_params["name"]
        backend = monkay.settings.backend
        form: FormData = await request.form()
        action: str | None = form.get("action")
        canonical_action = "remove" if action == "delete" else action

        # Page is retrieved to redirect the user back to the correct page after the action
        try:
            page: int = max(1, int(form.get("page", 1)))
        except ValueError:
            page = 1
        try:
            size: int = int(form.get("size", 20))
        except ValueError:
            size = 20
        if size not in DLQ_PAGE_SIZES:
            size = 20

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
            if canonical_action == "remove":
                add_message(request, "error", "You need to select a job to be deleted first.")
            else:
                add_message(request, "info", "You need to select a job to be retried first.")
            return RedirectResponse(self.get_return_url(request, name=queue), status_code=303)

        # 3. Process actions for selected IDs
        for job_id in job_ids:
            try:
                if canonical_action == "retry":
                    await backend.retry_job(queue, job_id)
                elif canonical_action == "remove":
                    await backend.remove_job(queue, job_id)
                else:
                    raise ValueError(f"Unknown action: {action}")
            except Exception as exc:
                record_audit_event(
                    request=request,
                    action=f"dlq.{canonical_action or 'unknown'}",
                    source="dlq.bulk",
                    status="failed",
                    queue=queue,
                    job_id=job_id,
                    error=str(exc),
                )
                continue

            record_audit_event(
                request=request,
                action=f"dlq.{canonical_action}",
                source="dlq.bulk",
                queue=queue,
                job_id=job_id,
            )

        # 4. Redirect back to the original page
        return RedirectResponse(f"{self.get_return_url(request, name=queue)}?page={page}&size={size}", status_code=303)
