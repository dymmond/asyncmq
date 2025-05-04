from lilya.requests import Request
from lilya.responses import HTMLResponse

from asyncmq.contrib.dashboard.engine import templates


async def dashboard_home(request: Request) -> HTMLResponse:
    return templates.get_template_response(request, "index.html", {"request": request, "title": "AsyncMQ Dashboard"})
