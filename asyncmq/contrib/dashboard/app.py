
from lilya.apps import Lilya
from lilya.requests import Request
from lilya.responses import HTMLResponse
from lilya.routing import RoutePath
from lilya.templating.controllers import templates

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views import dlq, home, jobs, metrics, queues, repeatables, workers


async def not_found(request: Request, exc: Exception) -> HTMLResponse:
    return templates.get_template_response(request, "404.html", context={"title": "Not Found"})


routes = [
    RoutePath("/", home.DashboardController, methods=["GET"], name="dashboard"),
    RoutePath("/queues/{name}/jobs", jobs.QueueJobController, methods=["GET"], name="queue-jobs"),
    RoutePath("/queues/{name}/repeatables", repeatables.RepeatablesController, methods=["GET"], name="repeatables"),
    RoutePath("/queues/{name}/dlq", dlq.DQLController, methods=["GET"], name="dlq"),
    RoutePath("/queues", queues.QueueController, methods=["GET"], name="queues"),
    RoutePath("/workers", workers.WorkerController, methods=["GET"], name="workers"),
    RoutePath("/metrics", metrics.MetricsController, methods=["GET"], name="metrics"),
]

app = Lilya(debug=settings.debug, routes=routes, exception_handlers={404: not_found})
