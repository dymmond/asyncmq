# asyncmq/contrib/dashboard/app.py

from typing import Any

from lilya.apps import Lilya
from lilya.requests import Request
from lilya.routing import RoutePath
from lilya.templating.controllers import templates  # noqa

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views import (
    dlq,
    home,
    jobs,
    metrics,
    queues,
    repeatables,
    workers,
)


async def not_found(request: Request, exc: Exception) -> Any:
    return templates.get_template_response(
        request,
        "404.html",
        context={"title": "Not Found"},
    )


routes = [
    # Home / Dashboard Overview
    RoutePath("/", home.DashboardController, methods=["GET"], name="dashboard"),

    # Queues list & detail (with pause/resume)
    RoutePath("/queues", queues.QueueController, methods=["GET"], name="queues"),
    RoutePath("/queues/{name}", queues.QueueDetailController, methods=["GET", "POST"], name="queue-detail"),

    # Jobs listing + pagination + Retry/Delete/Cancel
    RoutePath("/queues/{name}/jobs", jobs.QueueJobController, methods=["GET", "POST"], name="queue-jobs"),

    # Repeatable definitions
    RoutePath("/queues/{name}/repeatables", repeatables.RepeatablesController, methods=["GET"], name="repeatables"),

    # Dead-letter queue + Retry/Delete
    RoutePath("/queues/{name}/dlq", dlq.DLQController, methods=["GET", "POST"], name="dlq"),

    # Workers list
    RoutePath("/workers", workers.WorkerController, methods=["GET"], name="workers"),

    # Metrics overview
    RoutePath("/metrics", metrics.MetricsController, methods=["GET"], name="metrics"),
]

app = Lilya(
    debug=settings.debug,
    routes=routes,
    exception_handlers={404: not_found},
)
