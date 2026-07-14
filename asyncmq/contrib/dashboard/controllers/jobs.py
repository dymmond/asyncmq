from __future__ import annotations

import datetime as dt
import json
from typing import Any, cast
from urllib.parse import urlencode

from lilya.controllers import Controller
from lilya.datastructures import FormData
from lilya.requests import Request
from lilya.responses import JSONResponse, RedirectResponse, Response
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.audit import record_audit_event
from asyncmq.contrib.dashboard.engine import templates
from asyncmq.contrib.dashboard.messages import add_message
from asyncmq.contrib.dashboard.mixins import DashboardMixin

RawJobData = dict[str, Any]
FormattedJob = dict[str, Any]
JOB_STATE_TABS: list[tuple[str, str]] = [
    ("waiting", "Waiting"),
    ("active", "Active"),
    ("completed", "Completed"),
    ("failed", "Failed"),
    ("delayed", "Delayed"),
    ("repeatable", "Repeatable"),
]
JOB_DETAIL_STATES: tuple[str, ...] = tuple(key for key, _ in JOB_STATE_TABS)
SENSITIVE_KEY_PARTS: tuple[str, ...] = (
    "authorization",
    "api_key",
    "apikey",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)
MAX_DISPLAY_STRING_LENGTH = 4000
MAX_REDACTION_DEPTH = 8


def is_sensitive_key(key: str) -> bool:
    """Return whether a metadata key should be redacted before display."""
    lowered = key.lower()
    return any(part in lowered for part in SENSITIVE_KEY_PARTS)


def redact_for_display(value: Any, *, depth: int = 0) -> Any:
    """Return a bounded, redacted copy of runtime-owned job data."""
    if depth > MAX_REDACTION_DEPTH:
        return "[nested data omitted]"
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            text_key = str(key)
            redacted[text_key] = "[redacted]" if is_sensitive_key(text_key) else redact_for_display(
                item,
                depth=depth + 1,
            )
        return redacted
    if isinstance(value, list):
        return [redact_for_display(item, depth=depth + 1) for item in value]
    if isinstance(value, tuple):
        return [redact_for_display(item, depth=depth + 1) for item in value]
    if isinstance(value, str) and len(value) > MAX_DISPLAY_STRING_LENGTH:
        return f"{value[:MAX_DISPLAY_STRING_LENGTH]}...[truncated]"
    return value


def to_pretty_json(value: Any) -> str:
    """Serialize redacted display data as stable indented JSON."""
    return json.dumps(
        redact_for_display(value),
        indent=2,
        sort_keys=True,
        default=str,
    )


class QueueJobController(DashboardMixin, TemplateController):
    """
    Controller for viewing, paginating, and managing jobs within a specific queue.

    Handles job display filtered by state (waiting, active, failed, etc.) and bulk actions
    (retry, remove, cancel) via HTTP POST.
    """

    template_name: str = "jobs/jobs.html"

    def _get_params(self, request: Request) -> tuple[str, int, int, str, str, str, str]:
        """Extracts and validates query parameters for listing and filtering jobs."""
        state: str = request.query_params.get("state", "waiting")

        try:
            page: int = max(1, int(request.query_params.get("page", 1)))
            size: int = max(1, int(request.query_params.get("size", 20)))
        except ValueError:
            page, size = 1, 20

        q: str = request.query_params.get("q", "").strip()
        task: str = request.query_params.get("task", "").strip()
        job_id: str = request.query_params.get("job_id", "").strip()
        sort: str = request.query_params.get("sort", "newest").strip().lower()
        if sort not in {"newest", "oldest"}:
            sort = "newest"

        return state, page, size, q, task, job_id, sort

    def _extract_job_timestamp(self, raw_job: RawJobData) -> float:
        ts: Any = raw_job.get("run_at") or raw_job.get("created_at") or raw_job.get("timestamp") or 0
        try:
            return float(ts)
        except (TypeError, ValueError):
            return 0.0

    def _extract_job_task(self, raw_job: RawJobData) -> str:
        task: Any = raw_job.get("task_id") or raw_job.get("task") or raw_job.get("name") or ""
        return str(task)

    def _job_matches_filters(self, raw_job: RawJobData, *, q: str, task: str, job_id: str) -> bool:
        if job_id and job_id not in str(raw_job.get("id") or ""):
            return False

        task_value = self._extract_job_task(raw_job)
        if task and task.lower() not in task_value.lower():
            return False

        if q:
            searchable: str = json.dumps(raw_job, sort_keys=True, default=str).lower()
            if q.lower() not in searchable:
                return False

        return True

    @staticmethod
    def _build_query_string(**params: Any) -> str:
        normalized: dict[str, str] = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    continue
                normalized[key] = value
                continue
            normalized[key] = str(value)
        query = urlencode(normalized)
        return f"?{query}" if query else ""

    def _format_job_data(self, raw_job: RawJobData, *, state: str) -> FormattedJob:
        """Formats a single raw job dictionary for template display."""
        # Preference for timestamp fields
        ts: float = self._extract_job_timestamp(raw_job)

        try:
            # Convert Unix timestamp to human-readable format
            created: str = dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            created = "N/A"

        return {
            "id": raw_job.get("id"),
            "status": raw_job.get("status") or state,
            "task_id": self._extract_job_task(raw_job) or "n/a",
            "payload": raw_job,  # Full object for tojson() inspection
            "payload_json": to_pretty_json(raw_job),
            "run_at": raw_job.get("run_at"),  # Original timestamp (optional)
            "created_at": created,  # Formatted string
        }

    async def _fetch_and_paginate_jobs(
        self,
        queue: str,
        state: str,
        page: int,
        size: int,
        *,
        q: str,
        task: str,
        job_id: str,
        sort: str,
    ) -> tuple[list[FormattedJob], int, int, int]:
        """Fetches jobs for the given state, applies pagination, and calculates page counts."""
        backend: Any = monkay.settings.backend

        # Fetch all jobs for the given state
        try:
            all_jobs: list[RawJobData] = await backend.list_jobs(queue, state)
        except Exception:
            all_jobs = []

        filtered_jobs: list[RawJobData] = [
            job for job in all_jobs if self._job_matches_filters(job, q=q, task=task, job_id=job_id)
        ]
        filtered_jobs.sort(key=self._extract_job_timestamp, reverse=sort == "newest")
        total: int = len(filtered_jobs)

        total_pages: int = max(1, (total + size - 1) // size)
        page = min(page, total_pages)

        # Apply pagination slice
        start: int = (page - 1) * size
        end: int = start + size
        page_jobs: list[RawJobData] = filtered_jobs[start:end]

        # Format the sliced jobs
        jobs: list[FormattedJob] = [self._format_job_data(raw, state=state) for raw in page_jobs]

        return jobs, total, total_pages, page

    async def get(self, request: Request) -> Response:
        """
        Handles the GET request, retrieves filtered and paginated jobs, and renders the job list template.
        """
        queue: str = request.path_params["name"]

        # 1. Parameter Parsing
        state, page, size, q, task, job_id, sort = self._get_params(request)

        # 2. Fetch and Paginate
        jobs, total, total_pages, page = await self._fetch_and_paginate_jobs(
            queue,
            state,
            page,
            size,
            q=q,
            task=task,
            job_id=job_id,
            sort=sort,
        )

        tab_urls: dict[str, str] = {
            key: self._build_query_string(state=key, page=1, size=size, q=q, task=task, job_id=job_id, sort=sort)
            for key, _ in JOB_STATE_TABS
        }
        prev_page_url: str | None = (
            self._build_query_string(
                state=state,
                page=page - 1,
                size=size,
                q=q,
                task=task,
                job_id=job_id,
                sort=sort,
            )
            if page > 1
            else None
        )
        next_page_url: str | None = (
            self._build_query_string(
                state=state,
                page=page + 1,
                size=size,
                q=q,
                task=task,
                job_id=job_id,
                sort=sort,
            )
            if page < total_pages
            else None
        )

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
                "tabs": JOB_STATE_TABS,
                "q": q,
                "task": task,
                "job_id": job_id,
                "sort": sort,
                "tab_urls": tab_urls,
                "prev_page_url": prev_page_url,
                "next_page_url": next_page_url,
            }
        )
        return await self.render_template(request, context=context)

    async def post(self, request: Request) -> RedirectResponse:
        """
        Handles bulk actions (retry, remove, cancel) on selected job IDs.
        """
        queue: str = request.path_params["name"]
        backend: Any = monkay.settings.backend
        form: FormData = await request.form()
        action: str | None = form.get("action")
        canonical_action = "remove" if action == "delete" else action

        # Safely extract job IDs regardless of form submission format
        job_ids: list[str]
        if hasattr(form, "getlist"):
            job_ids = cast(list[str], cast(Any, form).getlist("job_id"))
        else:
            # Fallback for single value or a comma-delimited string
            raw: str = str(form.get("job_id") or "")
            job_ids = raw.split(",") if "," in raw else [raw]

        for job_id in job_ids:
            if not job_id:
                continue

            try:
                if canonical_action == "retry":
                    await backend.retry_job(queue, job_id)
                elif canonical_action == "remove":
                    await backend.remove_job(queue, job_id)
                elif canonical_action == "cancel":
                    await backend.cancel_job(queue, job_id)
                else:
                    raise ValueError(f"Unknown action: {action}")
            except Exception as exc:
                record_audit_event(
                    request=request,
                    action=f"job.{canonical_action or 'unknown'}",
                    source="jobs.bulk",
                    status="failed",
                    queue=queue,
                    job_id=job_id,
                    error=str(exc),
                )
                continue

            record_audit_event(
                request=request,
                action=f"job.{canonical_action}",
                source="jobs.bulk",
                queue=queue,
                job_id=job_id,
            )

        # Redirect back to the same list/state/page
        state: str = form.get("state", "waiting")
        q: str | None = form.get("q")
        task: str | None = form.get("task")
        job_id: str | None = form.get("job_id")
        sort: str | None = form.get("sort")
        size: str | None = form.get("size")
        redirect_query: str = self._build_query_string(
            state=state,
            page=1,
            size=size,
            q=q,
            task=task,
            job_id=job_id,
            sort=sort,
        )
        redirect_path = str(request.url_path_for("queue-jobs", name=queue))
        return RedirectResponse(f"{redirect_path}{redirect_query}", status_code=302)


class JobDetailController(DashboardMixin, TemplateController):
    """
    Renders a runtime-owned job detail page for a single queue job.

    The controller consumes backend inspection contracts (`get_job`,
    `get_job_state`, and `get_job_result`) and only falls back to bounded
    state-bucket inspection when a backend-shaped test double does not expose
    point lookup helpers.
    """

    template_name: str = "jobs/detail.html"

    async def _load_job(self, backend: Any, queue: str, job_id: str) -> RawJobData | None:
        """Load a job from the backend's native point lookup or inspection buckets."""
        if hasattr(backend, "get_job"):
            try:
                job = await backend.get_job(queue, job_id)
            except Exception:
                job = None
            if isinstance(job, dict):
                return dict(job)

        for state in JOB_DETAIL_STATES:
            try:
                jobs = await backend.list_jobs(queue, state)
            except Exception:
                jobs = []
            for job in jobs:
                current_id = job.get("id") or job.get("job_id")
                if current_id is not None and str(current_id) == job_id:
                    loaded = dict(job)
                    loaded.setdefault("state", state)
                    return loaded
        return None

    async def _get_backend_value(self, backend: Any, method: str, queue: str, job_id: str) -> Any:
        """Safely call an optional backend inspection method."""
        if not hasattr(backend, method):
            return None
        try:
            return await getattr(backend, method)(queue, job_id)
        except Exception:
            return None

    def _is_sensitive_key(self, key: str) -> bool:
        """Return whether a metadata key should be redacted before display."""
        return is_sensitive_key(key)

    def _redact_for_display(self, value: Any, *, depth: int = 0) -> Any:
        """Return a bounded, redacted copy of runtime-owned job data."""
        return redact_for_display(value, depth=depth)

    def _to_pretty_json(self, value: Any) -> str:
        """Serialize redacted display data as stable indented JSON."""
        return to_pretty_json(value)

    def _format_time(self, value: Any) -> str:
        """Format numeric timestamps while preserving backend-provided strings."""
        if value in (None, ""):
            return "-"
        try:
            return dt.datetime.fromtimestamp(float(value)).strftime("%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError, OSError):
            return str(value)

    def _extract_traceback(self, job: RawJobData) -> str | None:
        """Extract the most specific traceback-like field available on a job."""
        for key in ("error_traceback", "traceback", "stacktrace", "stack", "last_error"):
            value = job.get(key)
            if value:
                return str(value)
        return None

    async def get(self, request: Request) -> Response:
        """
        Render a single job detail page with metadata, payload, result, and diagnostics.
        """
        queue: str = request.path_params["name"]
        job_id: str = request.path_params["job_id"]
        backend: Any = monkay.settings.backend
        job = await self._load_job(backend, queue, job_id)

        if job is None:
            context = await self.get_context_data(request)
            context.update({"title": "Job Not Found", "url_prefix": context["url_prefix"]})
            return templates.get_template_response(request, "404.html", context=context, status_code=404)

        backend_state = await self._get_backend_value(backend, "get_job_state", queue, job_id)
        backend_result = await self._get_backend_value(backend, "get_job_result", queue, job_id)
        state = str(job.get("status") or job.get("state") or backend_state or "unknown")
        result = job.get("result") if "result" in job else backend_result
        task_id = str(job.get("task_id") or job.get("task") or job.get("name") or "n/a")
        traceback_text = self._extract_traceback(job)

        context: dict[str, Any] = await super().get_context_data(request)
        context.update(
            {
                "title": f"Job {job_id}",
                "active_page": "queues",
                "page_header": f"Job {job_id}",
                "queue": queue,
                "job": job,
                "job_id": job_id,
                "task_id": task_id,
                "state": state,
                "created_at": self._format_time(job.get("created_at") or job.get("timestamp")),
                "run_at": self._format_time(job.get("run_at") or job.get("delay_until")),
                "last_attempt": self._format_time(job.get("last_attempt")),
                "retries": job.get("retries", job.get("retry_count", "-")),
                "max_retries": job.get("max_retries", "-"),
                "depends_on": job.get("depends_on") or [],
                "args_json": self._to_pretty_json(job.get("args", [])),
                "kwargs_json": self._to_pretty_json(job.get("kwargs", {})),
                "payload_json": self._to_pretty_json(job),
                "result_json": self._to_pretty_json(result) if result is not None else None,
                "error_message": job.get("last_error") or job.get("error") or job.get("exception"),
                "traceback_text": traceback_text,
                "return_to": str(request.url_path_for("job-detail", name=queue, job_id=job_id)),
                "jobs_url": str(request.url_path_for("queue-jobs", name=queue)),
                "queue_url": str(request.url_path_for("queue-detail", name=queue)),
            }
        )
        return await self.render_template(request, context=context)


class JobActionController(Controller):
    """
    Handles single-job actions (retry, remove, cancel) via dedicated AJAX endpoints.
    """

    def _safe_return_to(self, request: Request, fallback: str, candidate: str | None) -> str:
        """Return a same-origin relative redirect target for HTML form actions."""
        if candidate and candidate.startswith("/") and not candidate.startswith("//"):
            return candidate
        referer = request.headers.get("referer")
        if referer:
            request_origin = f"{request.url.scheme}://{request.url.netloc}"
            if referer.startswith(request_origin):
                return referer[len(request_origin) :] or fallback
        return fallback

    async def _form_response_mode(self, request: Request) -> tuple[bool, str | None]:
        """Read optional HTML form response controls without affecting JSON callers."""
        content_type = request.headers.get("content-type", "")
        if "application/x-www-form-urlencoded" not in content_type and "multipart/form-data" not in content_type:
            return False, None
        form = await request.form()
        return form.get("_response") == "html", form.get("return_to")

    async def post(self, request: Request, job_id: str, action: str) -> JSONResponse | RedirectResponse:
        """
        Performs a single action on a job and returns a JSON status response.

        Args:
            request: The incoming Lilya Request object.
            job_id: The ID of the job to act upon (from the path).
            action: The action to perform ('retry', 'remove', or 'cancel') (from the path).

        Returns:
            JSONResponse: Status of the operation ({ok: true} on success).
        """
        queue: str = request.path_params["name"]
        backend: Any = monkay.settings.backend
        canonical_action = "remove" if action == "delete" else action
        html_response, return_to = await self._form_response_mode(request)
        fallback = str(request.url_path_for("job-detail", name=queue, job_id=job_id))

        try:
            if canonical_action == "retry":
                await backend.retry_job(queue, job_id)
            elif canonical_action == "remove":
                await backend.remove_job(queue, job_id)
            elif canonical_action == "cancel":
                await backend.cancel_job(queue, job_id)
            else:
                record_audit_event(
                    request=request,
                    action=f"job.{canonical_action}",
                    source="jobs.single",
                    status="failed",
                    queue=queue,
                    job_id=job_id,
                    error="Unknown action",
                )
                if html_response:
                    add_message(request, "error", "Unknown job action.")
                    return RedirectResponse(self._safe_return_to(request, fallback, return_to), status_code=303)
                return JSONResponse({"ok": False, "error": "Unknown action"}, status_code=400)
        except Exception as e:
            record_audit_event(
                request=request,
                action=f"job.{canonical_action}",
                source="jobs.single",
                status="failed",
                queue=queue,
                job_id=job_id,
                error=str(e),
            )
            if html_response:
                add_message(request, "error", f"Job {canonical_action} failed: {e}")
                return RedirectResponse(self._safe_return_to(request, fallback, return_to), status_code=303)
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

        record_audit_event(
            request=request,
            action=f"job.{canonical_action}",
            source="jobs.single",
            queue=queue,
            job_id=job_id,
        )
        if html_response:
            add_message(request, "success", f"Job '{job_id}' {canonical_action} completed.")
            return RedirectResponse(self._safe_return_to(request, fallback, return_to), status_code=303)
        return JSONResponse({"ok": True})
