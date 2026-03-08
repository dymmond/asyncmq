from __future__ import annotations

from typing import Any, Iterable

DEFAULT_JOB_TYPES: tuple[str, ...] = (
    "active",
    "completed",
    "delayed",
    "failed",
    "paused",
    "prioritized",
    "waiting",
    "waiting-children",
)

JOB_TYPE_ALIASES: dict[str, str] = {
    "wait": "waiting",
}


def normalize_job_type(job_type: str) -> str:
    """
    Normalize BullMQ-style job type aliases to AsyncMQ's inspection vocabulary.

    Args:
        job_type: The requested job type or state name.

    Returns:
        A canonical job type string that AsyncMQ inspection helpers understand.
    """
    return JOB_TYPE_ALIASES.get(job_type, job_type)


def sanitize_job_types(types: Iterable[str] | None) -> list[str]:
    """
    Expand and normalize a requested list of job types for queue inspection.

    BullMQ treats ``waiting`` as including the paused bucket when callers ask
    for counts across states. AsyncMQ does not materialize paused jobs into a
    separate storage bucket, but keeping the sanitized type list compatible
    preserves the public shape of count responses.

    Args:
        types: An optional iterable of requested job types.

    Returns:
        A de-duplicated list of canonical job types in caller-specified order,
        or the default BullMQ-compatible set when no types are provided.
    """
    current = [normalize_job_type(item) for item in types] if types else list(DEFAULT_JOB_TYPES)
    if "waiting" in current and "paused" not in current:
        current.append("paused")

    ordered: list[str] = []
    for item in current:
        if item not in ordered:
            ordered.append(item)
    return ordered


def has_pending_dependencies(job: dict[str, Any]) -> bool:
    """
    Determine whether a stored job still has unresolved parent dependencies.

    AsyncMQ stores dependency metadata on the job payload instead of
    representing it as a distinct backend state in every implementation. For
    inspection purposes this metadata is enough to expose a BullMQ-like
    ``waiting-children`` view.

    Args:
        job: The stored job payload.

    Returns:
        ``True`` when the job declares one or more unresolved parents.
    """
    depends_on = job.get("depends_on")
    return isinstance(depends_on, list) and len(depends_on) > 0


def matches_job_type(job: dict[str, Any], job_type: str) -> bool:
    """
    Check whether a stored job should appear in a requested inspection bucket.

    The helper intentionally distinguishes between runtime execution states and
    derived inspection views:

    - ``waiting-children`` is inferred from unresolved dependency metadata.
    - ``paused`` is not a persisted job state in AsyncMQ, so it always returns
      ``False`` and remains an intentional behavioral difference.
    - ``prioritized`` is also not a separate bucket because AsyncMQ models
      priority as ordering metadata on waiting jobs instead of a distinct state.

    Args:
        job: The stored job payload.
        job_type: The requested inspection bucket.

    Returns:
        ``True`` when the job belongs to the requested bucket.
    """
    requested = normalize_job_type(job_type)
    status = normalize_job_type(str(job.get("status") or ""))
    waiting_children = has_pending_dependencies(job)

    if requested == "waiting-children":
        return waiting_children
    if requested in {"paused", "prioritized"}:
        return False
    if waiting_children and requested in {"waiting", "delayed", "queued"}:
        return False
    return status == requested


def paginate_jobs(
    jobs: Iterable[dict[str, Any]],
    *,
    start: int = 0,
    end: int = -1,
    asc: bool = False,
) -> list[dict[str, Any]]:
    """
    Apply BullMQ-style pagination semantics to an ordered job sequence.

    Args:
        jobs: The ordered jobs to paginate.
        start: Zero-based inclusive start offset.
        end: Zero-based inclusive end offset. ``-1`` means "until the end".
        asc: Whether to preserve ascending order. When ``False``, the incoming
            order is reversed to approximate BullMQ's descending getter mode.

    Returns:
        The paginated list of jobs.
    """
    items = list(jobs)
    if not asc:
        items.reverse()
    if start < 0:
        start = 0
    if end < 0:
        return items[start:]
    return items[start : end + 1]


def infer_job_type(job: dict[str, Any]) -> str | None:
    """
    Infer the public inspection state for a stored job payload.

    Args:
        job: The stored job payload.

    Returns:
        The canonical inspection state, or ``None`` when the payload has no
        recognizable state information.
    """
    if has_pending_dependencies(job):
        return "waiting-children"

    status = job.get("status")
    if status is None:
        return None
    return normalize_job_type(str(status))
