from __future__ import annotations

import datetime as dt
import math
import time
from types import SimpleNamespace
from typing import Any, Mapping, cast

from lilya.requests import Request
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.mixins import DashboardMixin

WorkerDisplayInfo = dict[str, Any]
WORKER_PAGE_SIZES: tuple[int, ...] = (10, 20, 50, 100)


def _format_duration(seconds: int | float | None) -> str:
    """Return a compact duration label for heartbeat and TTL display."""
    if seconds is None:
        return "unknown"
    value = max(0, int(seconds))
    if value < 60:
        return f"{value}s"
    minutes, remainder = divmod(value, 60)
    if minutes < 60:
        return f"{minutes}m {remainder}s" if remainder and minutes < 10 else f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h" if hours else f"{days}d"


def _format_timestamp(timestamp: float | None) -> str:
    """Format a Unix timestamp for worker tables shown to operators."""
    if timestamp is None or timestamp <= 0:
        return "-"
    try:
        return dt.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, OverflowError, ValueError):
        return "-"


def _format_timestamp_iso(timestamp: float | None) -> str:
    """Format a Unix timestamp as ISO-8601 for semantic HTML attributes."""
    if timestamp is None or timestamp <= 0:
        return ""
    try:
        return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        return ""


def _get_worker_value(worker: Any, key: str, default: Any = None) -> Any:
    """Read a worker field from a mapping or WorkerInfo-like object."""
    if isinstance(worker, Mapping):
        return worker.get(key, default)
    return getattr(worker, key, default)


def _coerce_float(value: Any) -> float | None:
    """Convert runtime heartbeat values to floats without raising."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int:
    """Convert runtime concurrency values to non-negative integers."""
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


class WorkerController(DashboardMixin, TemplateController):
    """
    Controller for the Workers dashboard page.

    Displays a list of all active workers registered with the backend, including
    their queue assignment, concurrency level, and last recorded heartbeat time.
    Handles user-controlled pagination.
    """

    template_name: str = "workers/workers.html"

    def _health_for_age(self, age_seconds: int | None, heartbeat_ttl: float) -> tuple[str, str, str]:
        """Return status, label, and detail for a worker heartbeat age."""
        if age_seconds is None or heartbeat_ttl <= 0:
            return "unknown", "Unknown", "No heartbeat timestamp"
        if age_seconds > heartbeat_ttl:
            return "stale", "Stale", f"Older than TTL ({_format_duration(heartbeat_ttl)})"
        if age_seconds > heartbeat_ttl * 0.66:
            return "warning", "Aging", f"Near TTL ({_format_duration(heartbeat_ttl)})"
        return "healthy", "Healthy", "Heartbeat inside TTL"

    def _format_worker(self, worker: Any, *, now: float, heartbeat_ttl: float) -> WorkerDisplayInfo:
        """Normalize one backend-owned worker record for presentation."""
        if isinstance(worker, dict):
            worker = SimpleNamespace(**cast(dict[str, Any], worker))

        heartbeat = _coerce_float(_get_worker_value(worker, "heartbeat"))
        age_seconds = max(0, int(now - heartbeat)) if heartbeat is not None else None
        health_status, health_label, health_detail = self._health_for_age(age_seconds, heartbeat_ttl)
        concurrency = _coerce_int(_get_worker_value(worker, "concurrency", 0))

        return {
            "id": str(_get_worker_value(worker, "id", "unknown")),
            "queue": str(_get_worker_value(worker, "queue", "unknown")),
            "concurrency": concurrency,
            "heartbeat": _format_timestamp(heartbeat),
            "heartbeat_iso": _format_timestamp_iso(heartbeat),
            "heartbeat_raw": heartbeat,
            "heartbeat_age_seconds": age_seconds or 0,
            "heartbeat_age_label": f"{_format_duration(age_seconds)} ago" if age_seconds is not None else "unknown",
            "health_status": health_status,
            "health_label": health_label,
            "health_detail": health_detail,
        }

    def _parse_pagination(self, qs: Mapping[str, Any]) -> tuple[int, int]:
        """Parse worker pagination parameters and clamp page size to supported values."""
        try:
            page = max(1, int(qs.get("page", 1)))
        except ValueError:
            page = 1

        try:
            size = int(qs.get("size", 20))
        except ValueError:
            size = 20
        if size not in WORKER_PAGE_SIZES:
            size = 20
        return page, size

    def _summarize_workers(
        self,
        workers: list[WorkerDisplayInfo],
        *,
        heartbeat_ttl: float,
    ) -> dict[str, Any]:
        """Summarize worker records from the runtime for the page header."""
        healthy = sum(1 for worker in workers if worker["health_status"] == "healthy")
        warning = sum(1 for worker in workers if worker["health_status"] == "warning")
        stale = sum(1 for worker in workers if worker["health_status"] == "stale")
        capacity = sum(int(worker["concurrency"]) for worker in workers)
        queues = {worker["queue"] for worker in workers if worker["queue"] != "unknown"}
        return {
            "total": len(workers),
            "healthy": healthy,
            "attention": warning + stale,
            "capacity": capacity,
            "queues": len(queues),
            "ttl_label": _format_duration(heartbeat_ttl),
        }

    async def get(self, request: Request) -> Any:
        """
        Handles the GET request, retrieves active worker details, applies pagination,
        and renders the workers list page.

        Args:
            request: The incoming Lilya Request object.

        Returns:
            The rendered HTML response for the workers page.
        """
        context: dict[str, Any] = await super().get_context_data(request)

        backend: Any = monkay.settings.backend
        # worker_info is expected to be a list of dictionaries or objects
        worker_info: list[Any] = await backend.list_workers()
        now = time.time()
        heartbeat_ttl = max(0.0, float(getattr(monkay.settings, "heartbeat_ttl", 0) or 0))

        all_workers: list[WorkerDisplayInfo] = []
        for worker in worker_info:
            all_workers.append(self._format_worker(worker, now=now, heartbeat_ttl=heartbeat_ttl))

        # --- Pagination Logic ---
        qs: Mapping[str, Any] = request.query_params

        page, size = self._parse_pagination(qs)

        total: int = len(all_workers)

        # Calculate total pages, defaulting to 1 if total/size is 0
        total_pages: int = math.ceil(total / size) if total > 0 and size else 1

        # Clamp current page to the valid range
        page = min(page, total_pages) if total_pages > 0 else 1

        # Apply slicing
        start: int = (page - 1) * size
        end: int = start + size
        workers: list[WorkerDisplayInfo] = all_workers[start:end]

        # --- Context Update ---
        context.update(
            {
                "title": "Active Workers",
                "workers": workers,
                "worker_summary": self._summarize_workers(
                    all_workers,
                    heartbeat_ttl=heartbeat_ttl,
                ),
                "heartbeat_ttl": max(1, int(heartbeat_ttl)),
                "active_page": "workers",
                "page": page,
                "size": size,
                "total": total,
                "total_pages": total_pages,
                "page_sizes": list(WORKER_PAGE_SIZES),
                "page_header": "Active Workers",
            }
        )
        return await self.render_template(request, context=context)
