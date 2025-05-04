from lilya.requests import Request
from lilya.responses import HTMLResponse

from asyncmq.contrib.dashboard.engine import templates

dummy_queues = [
    {"name": "default", "waiting": 12, "active": 3, "completed": 100},
    {"name": "emails", "waiting": 7, "active": 1, "completed": 55},
]

async def queue_list(request: Request) -> HTMLResponse:
    return templates.get_template_response(request, "queues.html", {
        "request": request,
        "title": "Queues",
        "queues": dummy_queues
    })
