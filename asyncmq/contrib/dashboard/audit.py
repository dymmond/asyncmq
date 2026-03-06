from __future__ import annotations

import json
import threading
import time
from collections import deque
from typing import Any

from lilya.requests import Request

_AUDIT_MAX = 2000
_AUDIT_LOCK = threading.Lock()
_AUDIT_LOG: deque[dict[str, Any]] = deque(maxlen=_AUDIT_MAX)


def _extract_actor(request: Request | None) -> str | None:
    if request is None:
        return None
    user = getattr(request.state, "user", None)
    if user is None:
        return None
    if isinstance(user, dict):
        user_id = user.get("id")
        return str(user_id) if user_id is not None else None
    user_id = getattr(user, "id", None)
    return str(user_id) if user_id is not None else None


def record_audit_event(
    *,
    request: Request | None,
    action: str,
    source: str,
    status: str = "success",
    queue: str | None = None,
    job_id: str | None = None,
    details: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    event = {
        "timestamp": time.time(),
        "action": action,
        "source": source,
        "status": status,
        "queue": queue,
        "job_id": job_id,
        "actor": _extract_actor(request),
        "details": details or {},
        "error": error,
    }
    with _AUDIT_LOCK:
        _AUDIT_LOG.appendleft(event)


def list_audit_events(
    *,
    limit: int = 200,
    action: str | None = None,
    status: str | None = None,
    queue: str | None = None,
    q: str | None = None,
) -> list[dict[str, Any]]:
    text_q = (q or "").strip().lower()

    with _AUDIT_LOCK:
        events = list(_AUDIT_LOG)

    out: list[dict[str, Any]] = []
    for event in events:
        if action and event.get("action") != action:
            continue
        if status and event.get("status") != status:
            continue
        if queue and event.get("queue") != queue:
            continue

        if text_q:
            searchable = " ".join(
                [
                    str(event.get("action") or ""),
                    str(event.get("source") or ""),
                    str(event.get("queue") or ""),
                    str(event.get("job_id") or ""),
                    str(event.get("actor") or ""),
                    str(event.get("error") or ""),
                    json.dumps(event.get("details") or {}, sort_keys=True, default=str),
                ]
            ).lower()
            if text_q not in searchable:
                continue

        out.append(event)
        if len(out) >= max(1, limit):
            break

    return out


def clear_audit_events() -> None:
    with _AUDIT_LOCK:
        _AUDIT_LOG.clear()
