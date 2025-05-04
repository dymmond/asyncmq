from lilya.requests import Request
from lilya.responses import HTMLResponse

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.engine import templates

# Dummy data — will later pull from backend
repeatable_jobs = [
    {"task": "send_email", "interval": "5m", "last_run": "2024-05-03 15:00"},
    {"task": "refresh_cache", "cron": "*/10 * * * *", "last_run": "2024-05-03 15:10"},
]


async def repeatables_view(request: Request) -> HTMLResponse:
    queue = request.path_params.get("name", "default")
    return templates.get_template_response(
        request,
        "repeatables.html",
        {
            "request": request,
            "title": f"Repeatables – {queue}",
            "queue": queue,
            "repeatables": repeatable_jobs,
            "header_text": settings.dashboard_config.header_title,
            "favicon": settings.dashboard_config.favicon,
        },
    )
