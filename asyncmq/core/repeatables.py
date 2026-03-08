from __future__ import annotations

import hashlib
import json
from typing import Any

from asyncmq.jobs import Job

_EPHEMERAL_REPEATABLE_KEYS = {
    "id",
    "next_run",
    "paused",
    "queue",
    "queue_name",
    "repeat_id",
    "status",
    "_last_run",
}


def normalize_repeatable_job_def(job_def: dict[str, Any]) -> dict[str, Any]:
    """
    Return a canonical repeatable definition without transport-only fields.

    Repeatable definitions move through different backends, dashboards, and
    worker loops. Some of those surfaces attach operational metadata such as
    ``paused`` or ``next_run`` that should not affect schedule identity.

    Args:
        job_def: The raw repeatable definition received from a caller or backend.

    Returns:
        A cleaned dictionary containing only the logical schedule definition.
    """
    clean = {
        key: value for key, value in job_def.items() if key not in _EPHEMERAL_REPEATABLE_KEYS and value is not None
    }
    if "task" in clean and "task_id" not in clean:
        clean["task_id"] = clean.pop("task")
    return clean


def repeatable_identity(job_def: dict[str, Any]) -> str:
    """
    Build a stable identifier for a repeatable definition.

    The identifier is derived from a canonical JSON representation so backends
    can match schedules even when they serialize dictionaries differently or
    when callers include runtime-only fields such as ``paused``.

    Args:
        job_def: The repeatable definition to fingerprint.

    Returns:
        A stable string identifier suitable for backend storage keys.
    """
    canonical = json.dumps(normalize_repeatable_job_def(job_def), sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()
    return f"repeatable:{digest}"


def build_repeatable_job(job_def: dict[str, Any]) -> Job:
    """
    Materialize a queue job instance from a repeatable definition.

    Repeatable definitions store user-facing options such as ``retries`` and
    ``every``. This helper converts those scheduling options into a fresh
    ``Job`` payload for a single execution while preserving the intended
    execution settings for the generated job.

    Args:
        job_def: The logical repeatable definition.

    Returns:
        A new ``Job`` instance ready to be enqueued.
    """
    clean = normalize_repeatable_job_def(job_def)
    return Job(
        task_id=clean["task_id"],
        args=list(clean.get("args", [])),
        kwargs=dict(clean.get("kwargs", {})),
        retries=0,
        max_retries=int(clean.get("retries", clean.get("max_retries", 0)) or 0),
        backoff=clean.get("backoff"),
        ttl=clean.get("ttl"),
        priority=int(clean.get("priority", 5) or 5),
    )
