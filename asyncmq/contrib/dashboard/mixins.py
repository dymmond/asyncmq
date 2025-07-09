from typing import Any

from lilya.requests import Request

from asyncmq.conf import monkay
from asyncmq.contrib.dashboard.engine import templates
from asyncmq.contrib.dashboard.messages import get_messages


class DashboardMixin:
    templates = templates

    async def get_context_data(self, request: Request, **kwargs: Any) -> dict:
        context = {}
        context.update(
            {
                "title": monkay.settings.dashboard_config.title,
                "header_text": monkay.settings.dashboard_config.header_title,
                "favicon": monkay.settings.dashboard_config.favicon,
                "url_prefix": monkay.settings.dashboard_config.dashboard_url_prefix,
                "sidebar_bg_colour": monkay.settings.dashboard_config.sidebar_bg_colour,
                "messages": get_messages(request),
            }
        )
        return context
