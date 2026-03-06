from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

_HISTORY_MAX = 2000
_HISTORY_LOCK = threading.Lock()
_METRICS_HISTORY: deque[dict[str, Any]] = deque(maxlen=_HISTORY_MAX)


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def record_metrics_snapshot(
    *,
    metrics: dict[str, Any],
    counts: dict[str, Any] | None = None,
    total_queues: int | None = None,
    total_workers: int | None = None,
) -> None:
    counts = counts or {}
    snapshot = {
        "timestamp": time.time(),
        "throughput": _safe_int(metrics.get("throughput")),
        "avg_duration": metrics.get("avg_duration"),
        "retries": _safe_int(metrics.get("retries")),
        "failures": _safe_int(metrics.get("failures")),
        "waiting": _safe_int(counts.get("waiting")),
        "active": _safe_int(counts.get("active")),
        "delayed": _safe_int(counts.get("delayed")),
        "completed": _safe_int(counts.get("completed")),
        "failed": _safe_int(counts.get("failed")),
        "total_queues": _safe_int(total_queues),
        "total_workers": _safe_int(total_workers),
    }
    with _HISTORY_LOCK:
        _METRICS_HISTORY.appendleft(snapshot)


def list_metrics_history(*, limit: int = 120) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 500))
    with _HISTORY_LOCK:
        snapshots = list(_METRICS_HISTORY)[:safe_limit]

    output: list[dict[str, Any]] = []
    for row in snapshots:
        ts = float(row.get("timestamp", 0.0))
        output.append(
            {
                **row,
                "time": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            }
        )
    return output


def clear_metrics_history() -> None:
    with _HISTORY_LOCK:
        _METRICS_HISTORY.clear()
