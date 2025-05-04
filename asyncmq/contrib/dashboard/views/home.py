from lilya.requests import Request
from lilya.responses import HTMLResponse

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.engine import templates


async def dashboard_home(request: Request) -> HTMLResponse:
    return templates.get_template_response(
        request,
        "index.html",
        {
            "request": request,
            "title": settings.dashboard_config.title,
            "favicon": settings.dashboard_config.favicon,
            "header_text": settings.dashboard_config.header_title
        }
    )
