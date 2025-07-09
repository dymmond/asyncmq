from typing import Any

from lilya.apps import Lilya
from lilya.requests import Request
from lilya.routing import Include, RoutePath
from lilya.staticfiles import StaticFiles

from asyncmq.conf import monkay
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

dashboard = Lilya(
    debug=monkay.settings.debug,
    routes=[Include(path=monkay.settings.dashboard_config.dashboard_url_prefix, routes=routes)],
    exception_handlers={404: not_found},
)
