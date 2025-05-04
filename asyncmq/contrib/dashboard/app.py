from lilya.apps import Lilya
from lilya.requests import Request
from lilya.responses import HTMLResponse
from lilya.routing import RoutePath

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views import dlq, home, jobs, metrics, queues, repeatables, workers


async def not_found(request: Request, exc: Exception) -> HTMLResponse:
    return HTMLResponse("Not found", status_code=404)


routes = [
    RoutePath("/", home.dashboard_home, methods=["GET"], name="dashboard"),
    RoutePath("/queues/{name}/jobs", jobs.queue_jobs, methods=["GET"], name="queue-jobs"),
    RoutePath("/queues/{name}/repeatables", repeatables.repeatables_view, methods=["GET"], name="repeatables"),
    RoutePath("/queues/{name}/dlq", dlq.dlq_view, methods=["GET"], name="dlq"),
    RoutePath("/queues", queues.queue_list, methods=["GET"], name="queues"),
    RoutePath("/workers", workers.workers_view, methods=["GET"], name="workers"),
    RoutePath("/metrics", metrics.metrics_view, methods=["GET"], name="metrics"),
]

app = Lilya(debug=settings.debug, routes=routes, exception_handlers={404: not_found})
