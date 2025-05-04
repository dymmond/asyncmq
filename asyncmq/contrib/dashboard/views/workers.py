from lilya.requests import Request
from lilya.responses import HTMLResponse

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.engine import templates

# Dummy active workers data
worker_list = [
    {"id": "worker-a1", "queue": "default", "concurrency": 3, "heartbeat": "2024-05-04 11:30"},
    {"id": "worker-b2", "queue": "emails", "concurrency": 2, "heartbeat": "2024-05-04 11:31"},
]


async def workers_view(request: Request) -> HTMLResponse:
    return templates.get_template_response(
        request,
        "workers.html",
        {
            "request": request,
            "title": "Active Workers",
            "workers": worker_list,
            "header_text": settings.dashboard_config.header_title,
            "favicon": settings.dashboard_config.favicon,
        },
    )
