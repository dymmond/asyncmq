from lilya.apps import Lilya
from lilya.requests import Request
from lilya.responses import HTMLResponse
from lilya.routing import RoutePath

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.views import home, jobs, queues


async def not_found(request: Request, exc: Exception):
    return HTMLResponse("Not found", status_code=404)



routes = [
    RoutePath("/", home.dashboard_home, methods=["GET"], name="dashboard"),
    RoutePath("/queues", queues.queue_list, methods=["GET"], name="queues"),
    RoutePath("/jobs", jobs.queue_jobs, methods=["GET"], name="jobs"),
]

app = Lilya(debug=settings.debug, routes=routes, exception_handlers={404: not_found})
