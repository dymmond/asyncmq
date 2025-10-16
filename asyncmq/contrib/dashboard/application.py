from typing import Any, Awaitable, Callable, MutableMapping, cast

from lilya.apps import Lilya
from lilya.requests import Request
from lilya.routing import Include, RoutePath
from lilya.staticfiles import StaticFiles

from asyncmq import monkay
from asyncmq.contrib.dashboard.controllers import (
    dlq,
    home,
    jobs,
    metrics,
    queues,
    repeatables,
    sse,
    workers,
)
from asyncmq.contrib.dashboard.engine import templates  # noqa


class CompatibleURL:
    """URL-like object that provides make_absolute_url method for Lilya compatibility"""

    def __init__(self, path: str):
        self.path = path

    def make_absolute_url(self, base_url: str | None = None) -> str:
        return self.path

    def __str__(self) -> str:
        return self.path


class AsgiCompatibleRouter:
    """Router wrapper that provides path_for method for ASGI compatibility"""

    def __init__(self, original_router: Any, mount_path: str = "") -> None:
        self.original_router = original_router
        self.mount_path = mount_path

    def path_for(self, name: str, **path_params: Any) -> CompatibleURL:
        # Use configured dashboard prefix and the mount path for correctness under FastAPI mounts
        configured_prefix = monkay.settings.dashboard_config.dashboard_url_prefix or ""
        base = f"{self.mount_path}{configured_prefix}"
        if name == "statics":
            path = path_params.get("path", "")
            return CompatibleURL(f"{base}/static{path}")
        return CompatibleURL(f"{base}")

    def __getattr__(self, name: str) -> Any:
        return getattr(self.original_router, name)


class UnifiedDashboard:
    """Dashboard that works with both Lilya and FastAPI environments"""

    def __init__(self, lilya_app: Lilya) -> None:
        self.lilya_app = lilya_app
        # Expose the router for Lilya compatibility
        self.router = lilya_app.router

    async def __call__(
        self,
        scope: MutableMapping[str, Any],
        receive: Callable[[], Awaitable[MutableMapping[str, Any]]],
        send: Callable[[MutableMapping[str, Any]], Awaitable[None]],
    ) -> None:
        # Only apply FastAPI compatibility when we detect a FastAPI router
        if scope["type"] == "http" and scope.get("router") and not hasattr(scope.get("router"), "path_for"):
            # We're in a FastAPI context, apply compatibility
            scope = dict(scope)
            mount_path = self._extract_mount_path(scope)
            scope["router"] = AsgiCompatibleRouter(scope["router"], mount_path)

        await self.lilya_app(scope, receive, send)

    def _extract_mount_path(self, scope: MutableMapping[str, Any]) -> str:
        """Extract mount path from scope for proper URL generation.

        We rely on the ASGI-provided `root_path`, which is the standard
        mechanism set by Starlette/FastAPI when an application is mounted.
        """
        return cast(str, scope.get("root_path", "").rstrip("/"))

    def __getattr__(self, name: str) -> Any:
        """Delegate attributes to the underlying Lilya app for compatibility"""
        return getattr(self.lilya_app, name)


async def not_found(request: Request, exc: Exception) -> Any:
    return templates.get_template_response(
        request,
        "404.html",
        context={"title": "Not Found"},
        status_code=404,
    )


routes = [
    # Home / Dashboard Overview
    RoutePath("/", home.DashboardController, methods=["GET"], name="dashboard"),
    # Queues list & detail (with pause/resume)
    RoutePath("/queues", queues.QueueController, methods=["GET"], name="queues"),
    RoutePath("/queues/{name}", queues.QueueDetailController, methods=["GET", "POST"], name="queue-detail"),
    # Jobs listing + pagination + Retry/Delete/Cancel
    RoutePath("/queues/{name}/jobs", jobs.QueueJobController, methods=["GET", "POST"], name="queue-jobs"),
    RoutePath("/queues/{name}/jobs/{job_id}/{action}", jobs.JobActionController, methods=["POST"], name="job-action"),
    # Repeatable definitions
    RoutePath("/queues/{name}/repeatables", repeatables.RepeatablesController, methods=["GET"], name="repeatables"),
    RoutePath("/queues/{name}/repeatables/new", repeatables.RepeatablesNewController, methods=["GET", "POST"]),
    # Dead-letter queue + Retry/Delete
    RoutePath("/queues/{name}/dlq", dlq.DLQController, methods=["GET", "POST"], name="dlq"),
    # Workers list
    RoutePath("/workers", workers.WorkerController, methods=["GET"], name="workers"),
    # Metrics overview
    RoutePath("/metrics", metrics.MetricsController, methods=["GET"], name="metrics"),
    # New SSE endpoint for real-time updates
    RoutePath("/events", sse.SSEController, methods=["GET"], name="events"),
    # Serve the statics
    Include("/static", app=StaticFiles(packages=["asyncmq.contrib.dashboard"], html=True), name="statics"),
]

_lilya_dashboard = Lilya(
    debug=monkay.settings.debug,
    routes=[Include(path=monkay.settings.dashboard_config.dashboard_url_prefix, routes=routes)],
    exception_handlers={404: not_found},
)

# Create unified dashboard that works with both Lilya and FastAPI
dashboard: UnifiedDashboard = UnifiedDashboard(_lilya_dashboard)
