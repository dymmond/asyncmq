from lilya.apps import Lilya
from lilya.requests import Request
from lilya.responses import HTMLResponse
from lilya.routing import RoutePath

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views import dlq, home, jobs, metrics, queues, repeatables, workers


async def not_found(request: Request, exc: Exception) -> HTMLResponse:
    return HTMLResponse("Not found", status_code=404)


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
