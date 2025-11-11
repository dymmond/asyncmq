from typing import Any

from lilya.requests import Request

from asyncmq import monkay
from asyncmq.contrib.dashboard.engine import templates
from asyncmq.contrib.dashboard.messages import get_messages
from asyncmq.core.utils.dashboard import get_effective_prefix


def default_context(request: Request) -> dict:
    context = {}
    effective_prefix = get_effective_prefix(request)
    context.update(
        {
            "title": monkay.settings.dashboard_config.title,
            "header_text": monkay.settings.dashboard_config.header_title,
            "favicon": monkay.settings.dashboard_config.favicon,
            "url_prefix": effective_prefix,
            "sidebar_bg_colour": monkay.settings.dashboard_config.sidebar_bg_colour,
            "messages": get_messages(request),
        }
    )
    return context


class DashboardMixin:
    templates = templates

    async def get_context_data(self, request: Request, **kwargs: Any) -> dict:
        context = default_context(request)
        return context
