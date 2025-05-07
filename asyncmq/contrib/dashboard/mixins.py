from typing import Any

from lilya.requests import Request

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.engine import templates


class DashboardMixin:
    templates = templates

    async def get_context_data(self, request: Request, **kwargs: Any) -> dict:
        context = {}
        context.update(
            {
                "title": settings.dashboard_config.title,
                "header_text": settings.dashboard_config.header_title,
                "favicon": settings.dashboard_config.favicon,
                "url_prefix": settings.dashboard_config.dashboard_url_prefix,
            }
        )
        return context
