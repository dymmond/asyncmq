from typing import Any

from lilya.requests import Request

from asyncmq.contrib.dashboard.engine import templates
from asyncmq.contrib.dashboard.messages import get_messages
from asyncmq.core.dependencies import get_settings


class DashboardMixin:
    templates = templates

    async def get_context_data(self, request: Request, **kwargs: Any) -> dict:
        settings = get_settings()
        context = {}
        context.update(
            {
                "title": settings.dashboard_config.title,
                "header_text": settings.dashboard_config.header_title,
                "favicon": settings.dashboard_config.favicon,
                "url_prefix": settings.dashboard_config.dashboard_url_prefix,
                "sidebar_bg_colour": settings.dashboard_config.sidebar_bg_colour,
                "messages": get_messages(request),
            }
        )
        return context
