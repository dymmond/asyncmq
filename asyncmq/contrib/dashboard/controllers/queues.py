from __future__ import annotations

from typing import Any

from lilya.datastructures import URL, FormData
from lilya.requests import Request
from lilya.responses import RedirectResponse, Response
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.messages import add_message
from asyncmq.contrib.dashboard.mixins import DashboardMixin


class QueueController(DashboardMixin, TemplateController):
    """
    Renders the queue overview page, listing all available queues along with their job counts
    by state (waiting, active, delayed, failed, completed).

    Also handles POST requests to pause or resume a queue.
    """

    template_name: str = "queues/queues.html"

    def get_return_url(self, request: Request, reverse_name: str, **params: Any) -> URL:
        """Calculates the URL path for redirecting back to this controller."""
        return request.url_path_for(reverse_name, **params)

    async def get_queues(self) -> list[dict[str, Any]]:
        """
        Retrieves all registered queues and fetches the job counts and pause state for each one.

        Returns:
            A list of dictionaries, where each dictionary contains the queue name,
            paused status, and count for all major job states.
        """
        backend: Any = monkay.settings.backend
        queues: list[str] = await backend.list_queues()
        job_states: tuple[str, ...] = (
            "waiting",
            "active",
            "delayed",
            "failed",
            "completed",
        )

        rows: list[dict[str, Any]] = []
        for q in queues:
            # Check for paused state (requires backend support)
            paused: bool = False
            if hasattr(backend, "is_queue_paused"):
                paused = await backend.is_queue_paused(q)

            # Get counts by state
            counts: dict[str, int] = {}
            for state in job_states:
                jobs: list[Any] = await backend.list_jobs(q, state)
                counts[state] = len(jobs)

            rows.append(
                {
                    "name": q,
                    "paused": paused,
                    "waiting": counts["waiting"],
                    "active": counts["active"],
                    "delayed": counts["delayed"],
                    "failed": counts["failed"],
                    "completed": counts["completed"],
                }
            )
        return rows

    async def get(self, request: Request) -> Response:
        """
        Handles the GET request, retrieves all queue data, and renders the overview page.
        """
        context: dict[str, Any] = await self.get_context_data(request)
        queues: list[dict[str, Any]] = await self.get_queues()

        context.update(
            {
                "title": "Queues",
                "queues": queues,
                "active_page": "queues",
                "page_header": "Overview",
            }
        )
        return await self.render_template(request, context=context)

    async def post(self, request: Request) -> RedirectResponse:
        """
        Handles pause/resume actions submitted via POST requests.

        Args:
            request: The incoming request object containing path and form data.

        Returns:
            A redirect response back to the queue detail page.
        """
        backend: Any = monkay.settings.backend
        q: str = request.path_params["name"]
        form: FormData = await request.form()
        action: str | None = form.get("action")

        if action == "pause" and hasattr(backend, "pause_queue"):
            await backend.pause_queue(q)
            add_message(request, "success", f"Queue '{q}' paused.")
        elif action == "resume" and hasattr(backend, "resume_queue"):
            await backend.resume_queue(q)
            add_message(request, "success", f"Queue '{q}' resumed.")

        # Redirect to the queue detail using the named route
        return RedirectResponse(self.get_return_url(request, "queue-detail", name=q), status_code=303)


class QueueDetailController(DashboardMixin, TemplateController):
    """
    Shows detailed information for a single queue, including job counts by state
    and its current paused status. Allows form submission for pause/resume actions.
    """

    template_name: str = "queues/info.html"

    def get_return_url(self, request: Request, reverse_name: str, **params: Any) -> URL:
        """Calculates the URL path for redirecting back to this controller."""
        return request.url_path_for(reverse_name, **params)

    async def get(self, request: Request) -> Response:
        """
        Handles the GET request, retrieves details and job counts for the specified queue,
        and renders the detail page.
        """
        backend: Any = monkay.settings.backend
        q: str = request.path_params["name"]
        job_states: tuple[str, ...] = (
            "waiting",
            "active",
            "delayed",
            "failed",
            "completed",
        )

        # Get paused state
        paused: bool = False
        if hasattr(backend, "is_queue_paused"):
            paused = await backend.is_queue_paused(q)

        # Get counts by state
        counts: dict[str, int] = {}
        for state in job_states:
            jobs: list[Any] = await backend.list_jobs(q, state)
            counts[state] = len(jobs)

        context: dict[str, Any] = await self.get_context_data(request)
        context.update(
            {
                "title": f"Queue '{q}'",
                "paused": paused,
                "counts": counts,
                "active_page": "queues",
                "page_header": f"{q} details",
                "queue": q,
            }
        )

        return await self.render_template(request, context=context)

    async def post(self, request: Request) -> RedirectResponse:
        """
        Handles form POSTs from the pause/resume buttons on the detail page.
        """
        backend: Any = monkay.settings.backend
        q: str = request.path_params["name"]
        form: FormData = await request.form()
        action: str | None = form.get("action")

        if action == "pause" and hasattr(backend, "pause_queue"):
            await backend.pause_queue(q)
            add_message(request, "success", f"Queue '{q}' paused.")
        elif action == "resume" and hasattr(backend, "resume_queue"):
            await backend.resume_queue(q)
            add_message(request, "success", f"Queue '{q}' resumed.")

        # Redirect back to the detail page itself
        return RedirectResponse(self.get_return_url(request, "queue-detail", name=q), status_code=303)
