from __future__ import annotations

from typing import Any

from lilya.controllers import Controller
from lilya.requests import Request
from lilya.responses import JSONResponse, TextResponse
from lilya.templating.controllers import TemplateController

from asyncmq import monkay
from asyncmq.contrib.dashboard.controllers._counts import (
    JOB_STATES,
    aggregate_counts,
    get_queue_state_counts,
)
from asyncmq.contrib.dashboard.metrics_history import list_metrics_history, record_metrics_snapshot
from asyncmq.contrib.dashboard.mixins import DashboardMixin

PROMETHEUS_MEDIA_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _prometheus_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _prometheus_sample(name: str, value: int | float, labels: dict[str, str] | None = None) -> str:
    if not labels:
        return f"{name} {value}"
    label_text = ",".join(f'{key}="{_prometheus_escape(label)}"' for key, label in sorted(labels.items()))
    return f"{name}{{{label_text}}} {value}"


class MetricsController(DashboardMixin, TemplateController):
    """
    Controller for the Metrics dashboard page.

    This controller aggregates job statistics across all available queues and job states
    to provide high-level metrics like throughput and failure counts.
    """

    template_name: str = "metrics/metrics.html"

    async def get(self, request: Request) -> Any:
        """
        Handles the GET request, retrieves and aggregates job counts, and renders the metrics dashboard.

        Args:
            request: The incoming Lilya Request object.

        Returns:
            The rendered HTML response for the metrics page.
        """
        # 1. Base context (title, header, favicon)
        context: dict[str, Any] = await super().get_context_data(request)

        # 2. Fetch all queues
        backend: Any = monkay.settings.backend
        try:
            queues: list[str] = await backend.list_queues()
        except Exception:
            queues = []
        total_queues = len(queues)

        counts = (
            await aggregate_counts(backend, queues)
            if queues
            else {"waiting": 0, "active": 0, "completed": 0, "failed": 0, "delayed": 0}
        )
        try:
            total_workers = len(await backend.list_workers())
        except Exception:
            total_workers = 0

        # 4. Build the metrics payload for the template
        metrics: dict[str, Any] = {
            "throughput": counts["completed"],
            "avg_duration": None,  # TODO: compute from timestamps
            "retries": counts["failed"],
            "failures": counts["failed"],
        }
        record_metrics_snapshot(
            metrics=metrics,
            counts=counts,
            total_queues=total_queues,
            total_workers=total_workers,
        )

        # 5. Inject and render
        context.update(
            {
                "title": "Metrics",
                "metrics": metrics,
                "counts": counts,
                "total_queues": total_queues,
                "total_workers": total_workers,
                "metrics_history": list_metrics_history(limit=30),
                "active_page": "metrics",
                "page_header": "System Metrics",
            }
        )
        return await self.render_template(request, context=context)


class MetricsHistoryController(Controller):
    async def get(self, request: Request) -> JSONResponse:
        limit_raw = request.query_params.get("limit", "120")
        try:
            limit = int(limit_raw)
        except ValueError:
            limit = 120
        limit = max(1, min(limit, 500))
        return JSONResponse({"history": list_metrics_history(limit=limit)})


class PrometheusMetricsController(Controller):
    async def get(self, request: Request) -> TextResponse:
        backend: Any = monkay.settings.backend
        lines = [
            "# HELP asyncmq_dashboard_ready 1 when dashboard metrics collection can inspect the backend.",
            "# TYPE asyncmq_dashboard_ready gauge",
        ]
        try:
            queues = sorted(await backend.list_queues())
            workers = await backend.list_workers()
        except Exception as exc:
            lines.extend(
                [
                    _prometheus_sample("asyncmq_dashboard_ready", 0),
                    f"# asyncmq_dashboard_error {type(exc).__name__}: {_prometheus_escape(str(exc))}",
                ]
            )
            return TextResponse("\n".join(lines) + "\n", status_code=503, media_type=PROMETHEUS_MEDIA_TYPE)

        lines.extend(
            [
                _prometheus_sample("asyncmq_dashboard_ready", 1),
                "# HELP asyncmq_queue_total Number of queues visible to the dashboard backend.",
                "# TYPE asyncmq_queue_total gauge",
                _prometheus_sample("asyncmq_queue_total", len(queues)),
                "# HELP asyncmq_worker_total Number of workers visible to the dashboard backend.",
                "# TYPE asyncmq_worker_total gauge",
                _prometheus_sample("asyncmq_worker_total", len(workers)),
                "# HELP asyncmq_queue_jobs Number of jobs by queue and state.",
                "# TYPE asyncmq_queue_jobs gauge",
            ]
        )

        for queue in queues:
            counts = await get_queue_state_counts(backend, queue)
            for state in JOB_STATES:
                lines.append(
                    _prometheus_sample(
                        "asyncmq_queue_jobs",
                        counts[state],
                        {"queue": queue, "state": state},
                    )
                )

        return TextResponse("\n".join(lines) + "\n", media_type=PROMETHEUS_MEDIA_TYPE)
