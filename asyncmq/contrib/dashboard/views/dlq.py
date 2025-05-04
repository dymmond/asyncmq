from lilya.requests import Request
from lilya.responses import HTMLResponse

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.engine import templates

# Dummy DLQ jobs
dlq_jobs = [
    {"id": "job404", "error": "TimeoutError", "created_at": "2024-05-03 14:30"},
    {"id": "job408", "error": "ConnectionRefused", "created_at": "2024-05-03 14:35"},
]


async def dlq_view(request: Request) -> HTMLResponse:
    queue = request.path_params.get("name", "default")
    return templates.get_template_response(
        request,
        "dlq.html",
        {
            "request": request,
            "title": f"Dead Letter Queue – {queue}",
            "queue": queue,
            "jobs": dlq_jobs,
            "header_text": settings.dashboard_config.header_title,
            "favicon": settings.dashboard_config.favicon,
        },
    )
