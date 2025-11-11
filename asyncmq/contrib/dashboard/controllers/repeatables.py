from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from lilya.datastructures import FormData
from lilya.requests import Request
from lilya.responses import RedirectResponse, Response
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.mixins import DashboardMixin
from asyncmq.queues import Queue


class RepeatablesController(DashboardMixin, TemplateController):
    """
    Controller for viewing and managing existing repeatable (scheduled/periodic) jobs
    for a specific queue.

    Handles listing, viewing details, and bulk actions (pause/resume/remove).
    """

    template_name: str = "repeatables/repeatables.html"

    async def get_repeatables(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Retrieves raw repeatable job definitions from the backend and formats them
        for template rendering.

        Handles variations in backend storage format (dict vs. object).

        Args:
            queue_name: The name of the queue to fetch repeatables for.

        Returns:
            A list of dictionaries containing formatted repeatable job details.
        """
        backend: Any = monkay.settings.backend
        # Assuming backend.list_repeatables returns a list of raw job definitions/objects
        repeatables: list[Any] = await backend.list_repeatables(queue_name)

        rows: list[dict[str, Any]] = []
        for rec in repeatables:
            # Normalize the raw record (rec) into job definition data (jd)
            if isinstance(rec, dict):
                jd: dict[str, Any] = rec
                raw_next_run: Any = rec.get("next_run")
                paused: bool = bool(rec.get("paused", False))
            else:
                # Handle objects returned by some backends
                jd = getattr(rec, "job_def", {})
                raw_next_run = getattr(rec, "next_run", None)
                paused = bool(getattr(rec, "paused", False))

            task_id: str | None = jd.get("task_id") or jd.get("name")
            every: Any = jd.get("every")
            cron: Any = jd.get("cron")

            # Format next run time
            next_run: str
            try:
                if raw_next_run:
                    # Convert Unix timestamp or datetime-like object to formatted string
                    next_run = datetime.fromtimestamp(raw_next_run).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    next_run = "—"
            except Exception:
                next_run = "—"

            rows.append(
                {
                    "task_id": task_id,
                    "every": every,
                    "cron": cron,
                    "next_run": next_run,
                    "paused": paused,
                    "job_def": jd,  # Full job definition included for POST actions
                }
            )
        return rows

    async def get(self, request: Request) -> Response:
        """
        Handles the GET request, retrieves and renders the list of repeatables for the queue.
        """
        queue: str = request.path_params["name"]
        repeatables: list[dict[str, Any]] = await self.get_repeatables(queue)

        ctx: dict[str, Any] = await super().get_context_data(request)
        ctx.update(
            {
                "title": f"Repeatables — {queue}",
                "page_header": f"Repeatable Jobs for “{queue}”",
                "queue": queue,
                "repeatables": repeatables,
            }
        )
        return await self.render_template(request, context=ctx)

    async def post(self, request: Request) -> RedirectResponse:
        """
        Handles pause, resume, and remove actions for a single repeatable job.

        Actions rely on the full `job_def` (sent as JSON from a hidden form field)
        being passed back to the backend.
        """
        form: FormData = await request.form()
        queue: str = request.path_params["name"]
        action: str | None = form.get("action")

        # Job definition is sent as a JSON string
        job_def: dict[str, Any] = json.loads(form["job_def"])
        backend: Any = monkay.settings.backend

        if action == "pause":
            await backend.pause_repeatable(queue, job_def)
        elif action == "resume":
            await backend.resume_repeatable(queue, job_def)
        elif action == "remove":
            try:
                raw: str = json.dumps(job_def)
                del backend.repeatables[queue][raw]
            except KeyError:
                pass
        else:
            # unknown action
            pass

        # Redirect back to GET, preserving query string (state, page, etc.)
        qs: str = request.url.query
        url: str = request.url.path + (f"?{qs}" if qs else "")
        return RedirectResponse(url, status_code=303)


class RepeatablesNewController(DashboardMixin, TemplateController):
    """
    Controller for the page used to define and add a new repeatable job.
    """

    template_name: str = "repeatables/new.html"

    def get_default_job_def(self, queue: str) -> dict[str, Any]:
        """
        Provides a default structure for the job definition form.
        """
        return {"queue": queue, "task_id": "", "every": None, "cron": None}

    async def get(self, request: Request) -> Response:
        """
        Handles the GET request and renders the form for creating a new repeatable job.
        """
        queue: str = request.path_params["name"]
        ctx: dict[str, Any] = await super().get_context_data(request)
        ctx.update(
            {
                "page_header": f"New Repeatable — {queue}",
                "queue": queue,
                "job_def": self.get_default_job_def(queue),
            }
        )
        return await self.render_template(request, context=ctx)

    async def post(self, request: Request) -> RedirectResponse:
        """
        Handles form POST submission, creates a new repeatable job, and redirects
        back to the main repeatables list.
        """
        form: FormData = await request.form()
        queue: Queue = Queue(request.path_params["name"])

        # 1. Build job_def from form data
        jd: dict[str, Any] = {"task_id": "", "every": None, "cron": None}

        jd["task_id"] = form.get("task_id", "")

        if form.get("every"):
            jd["every"] = int(form["every"])
        if form.get("cron"):
            jd["cron"] = form["cron"]

        # Filter out None values before submission
        data: dict[str, Any] = {k: v for k, v in jd.items() if v is not None}

        # 2. Add repeatable job to the queue
        queue.add_repeatable(**data)

        # 3. Redirect back to the main list
        qs: str = request.url.query

        # Remove "/new" from the path
        url: str = request.url.path.rsplit("/new", 1)[0]
        if qs:
            url = f"{url}?{qs}"

        return RedirectResponse(url, status_code=303)
