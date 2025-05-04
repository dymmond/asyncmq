from lilya.requests import Request
from lilya.responses import HTMLResponse

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.engine import templates

# Dummy data simulating metrics
metrics_data = {"throughput": "42 jobs/min", "avg_duration": "230ms", "retries": 12, "failures": 5}


async def metrics_view(request: Request) -> HTMLResponse:
    return templates.get_template_response(
        request,
        "metrics.html",
        {
            "request": request,
            "title": "System Metrics",
            "metrics": metrics_data,
            "header_text": settings.dashboard_config.header_title,
            "favicon": settings.dashboard_config.favicon,
        },
    )
