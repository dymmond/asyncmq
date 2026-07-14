from typing import Any

from lilya.requests import Request

from asyncmq import monkay
from asyncmq.contrib.dashboard.engine import templates
from asyncmq.contrib.dashboard.messages import get_messages
from asyncmq.core.utils.dashboard import get_effective_prefix


def _prefix_static_url(value: str, effective_prefix: str) -> str:
    """Return a prefix-aware URL for dashboard-owned static assets."""
    if value.startswith(("http://", "https://", "data:")):
        return value
    if value.startswith("/static/") and effective_prefix:
        return f"{effective_prefix}{value}"
    return value


def default_context(request: Request) -> dict:
    """Build the common template context shared by dashboard pages."""
    context = {}
    effective_prefix = get_effective_prefix(request)
    favicon = _prefix_static_url(monkay.settings.dashboard_config.favicon, effective_prefix)
    context.update(
        {
            "title": monkay.settings.dashboard_config.title,
            "header_text": monkay.settings.dashboard_config.header_title,
            "favicon": favicon,
            "url_prefix": effective_prefix,
            "sidebar_bg_colour": monkay.settings.dashboard_config.sidebar_bg_colour,
            "messages": get_messages(request),
        }
    )
    return context


class DashboardMixin:
    """Provides shared Jinja templates and base context for dashboard controllers."""

    templates = templates

    async def get_context_data(self, request: Request, **kwargs: Any) -> dict:
        """Return the standard dashboard context for a template response."""
        context = default_context(request)
        return context
