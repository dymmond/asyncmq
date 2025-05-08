from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from lilya.requests import Request
from lilya.responses import RedirectResponse
from lilya.templating.controllers import TemplateController

from asyncmq.conf import monkay
from asyncmq.contrib.dashboard.mixins import DashboardMixin
from asyncmq.queues import Queue


class RepeatablesController(DashboardMixin, TemplateController):
    template_name = "repeatables/repeatables.html"

    async def get_repeatables(self, queue_name: str) -> list[dict[str, Any]]:
        backend = monkay.settings.backend
        repeatables = await backend.list_repeatables(queue_name)

        rows: list[dict[str, Any]] = []
        for rec in repeatables:
            jd = rec.job_def
            task_id = jd.get("task_id") or jd.get("name")
            every = jd.get("every")
            cron = jd.get("cron")

            # format next run
            try:
                next_run = datetime.fromtimestamp(rec.next_run).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                next_run = "—"

            rows.append(
                {
                    "task_id": task_id,
                    "every": every,
                    "cron": cron,
                    "next_run": next_run,
                    "paused": rec.paused,
                    # keep the raw job_def around for the form
                    **{"job_def": jd},
                }
            )
        return rows

    async def get(self, request: Request) -> Any:
        queue = request.path_params["name"]
        repeatables = await self.get_repeatables(queue)

        ctx = await super().get_context_data(request)
        ctx.update(
            {
                "title": f"Repeatables — {queue}",
                "page_header": f"Repeatable Jobs for “{queue}”",
                "queue": queue,
                "repeatables": repeatables,
            }
        )
        return await self.render_template(request, context=ctx)

    async def post(self, request: Request) -> Any:
        form = await request.form()
        queue = request.path_params["name"]
        action = form.get("action")
        # job_def was JSON‐embedded in a hidden field
        job_def = json.loads(form["job_def"])
        backend = monkay.settings.backend

        if action == "pause":
            await backend.pause_repeatable(queue, job_def)
        elif action == "resume":
            await backend.resume_repeatable(queue, job_def)
        elif action == "remove":
            # note: you need to add remove_repeatable() in your backend,
            # for now we just un‐pause and drop it from the in‐memory dict:
            try:
                raw = json.dumps(job_def)
                del backend.repeatables[queue][raw]
            except KeyError:
                pass
        else:
            # unknown action
            pass

        # redirect back to GET (carries queue/state/page via query string if present)
        qs = request.url.query
        url = request.url.path + (f"?{qs}" if qs else "")
        return RedirectResponse(url, status_code=303)


class RepeatablesNewController(DashboardMixin, TemplateController):
    template_name = "repeatables/new.html"

    def get_default_job_def(self, queue: str) -> dict[str, Any]:
        return {"queue": queue, "task_id": "", "every": None, "cron": None}

    async def get(self, request: Request) -> Any:
        queue = request.path_params["name"]
        ctx = await super().get_context_data(request)
        ctx.update(
            {
                "page_header": f"New Repeatable — {queue}",
                "queue": queue,
                "job_def": self.get_default_job_def(queue),
            }
        )
        return await self.render_template(request, context=ctx)

    async def post(self, request: Request) -> Any:
        form = await request.form()
        queue = Queue(request.path_params["name"])

        # build job_def from form
        jd: dict[str, Any] = {"task_id": "", "every": None, "cron": None}

        jd["task_id"] = form.get("task_id", "")
        if form.get("every"):
            jd["every"] = int(form["every"])
        if form.get("cron"):
            jd["cron"] = form["cron"]

        data = {k: v for k, v in jd.items() if v is not None}

        queue.add_repeatable(**data)

        qs = request.url.query
        url = request.url.path.rsplit("/new", 1)[0]
        if qs:
            url = f"{url}?{qs}"
        return RedirectResponse(url, status_code=303)
