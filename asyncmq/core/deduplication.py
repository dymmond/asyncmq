from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from asyncmq.core.inspection import infer_job_type

TERMINAL_JOB_TYPES = {"completed", "failed", "expired"}


def normalize_deduplication_options(
    deduplication: Mapping[str, Any] | None,
    debounce: Mapping[str, Any] | None,
    *,
    delay: float | None,
) -> dict[str, Any] | None:
    """
    Normalize BullMQ-style deduplication options into AsyncMQ's stored shape.

    AsyncMQ accepts BullMQ's modern ``deduplication`` option and the older
    ``debounce`` alias. The normalized dictionary is stored on the job payload
    so every backend can evaluate the same rules without Redis-specific
    metadata structures.

    Args:
        deduplication: The preferred BullMQ-style deduplication mapping.
        debounce: The deprecated debounce alias. It is accepted for migration
            compatibility and normalized into the same internal structure.
        delay: The producer-level delay requested for this job, if any. Replace
            semantics only take effect while the current deduplicated job is
            still delayed, mirroring BullMQ's debounce behavior.

    Returns:
        A normalized dictionary ready to be attached to a job payload, or
        ``None`` when deduplication is not configured.

    Raises:
        TypeError: If the provided option is not a mapping or contains values
            of the wrong type.
        ValueError: If the deduplication identifier is missing or invalid.
    """
    raw = deduplication if deduplication is not None else debounce
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise TypeError("Deduplication options must be provided as a mapping.")

    deduplication_id = raw.get("id")
    if not isinstance(deduplication_id, str) or not deduplication_id:
        raise ValueError("Deduplication options require a non-empty string 'id'.")

    ttl = raw.get("ttl")
    if ttl is not None:
        if not isinstance(ttl, (int, float)):
            raise TypeError("Deduplication 'ttl' must be numeric when provided.")
        if float(ttl) <= 0:
            raise ValueError("Deduplication 'ttl' must be greater than zero.")

    extend = bool(raw.get("extend", False))
    replace = bool(raw.get("replace", False))
    normalized_ttl = float(ttl) if ttl is not None else None
    expires_at = time.time() + normalized_ttl if normalized_ttl is not None else None

    if replace and delay is None:
        mode = "replace"
    elif replace and delay is not None:
        mode = "debounce"
    elif normalized_ttl is not None:
        mode = "throttle"
    else:
        mode = "simple"

    return {
        "id": deduplication_id,
        "ttl": normalized_ttl,
        "extend": extend,
        "replace": replace,
        "delay": delay,
        "mode": mode,
        "expires_at": expires_at,
    }


def extract_deduplication(job: Mapping[str, Any]) -> dict[str, Any] | None:
    """
    Return normalized deduplication metadata from a stored job payload.

    Args:
        job: The stored job payload.

    Returns:
        The deduplication metadata if present and valid enough to evaluate,
        otherwise ``None``.
    """
    raw = job.get("deduplication")
    if not isinstance(raw, Mapping):
        return None
    deduplication_id = raw.get("id")
    if not isinstance(deduplication_id, str) or not deduplication_id:
        return None
    return dict(raw)


def is_deduplication_active(job: Mapping[str, Any], deduplication_id: str, *, now: float | None = None) -> bool:
    """
    Determine whether a stored job still owns a deduplication window.

    Simple mode stays active while the job is not in a terminal state.
    TTL-backed modes stay active until the stored expiration timestamp, even if
    the job already completed, which matches BullMQ's throttle/debounce model.

    Args:
        job: The stored job payload to inspect.
        deduplication_id: The deduplication identifier being queried.
        now: Optional timestamp override used by tests or callers that already
            computed a reference time.

    Returns:
        ``True`` when the payload still owns the deduplication key.
    """
    deduplication = extract_deduplication(job)
    if deduplication is None or deduplication.get("id") != deduplication_id:
        return False

    current_time = time.time() if now is None else now
    expires_at = deduplication.get("expires_at")
    if isinstance(expires_at, (int, float)):
        return float(expires_at) > current_time

    return infer_job_type(dict(job)) not in TERMINAL_JOB_TYPES


def select_deduplication_owner(
    jobs: list[dict[str, Any]],
    deduplication_id: str,
    *,
    now: float | None = None,
) -> dict[str, Any] | None:
    """
    Select the job payload that currently owns a deduplication identifier.

    AsyncMQ should normally maintain only one active owner for a given
    deduplication identifier. If a backend contains multiple historical
    matches, the newest active payload wins so callers can recover
    deterministically from partial failures or manual state edits.

    Args:
        jobs: Candidate job payloads from queue inspection.
        deduplication_id: The deduplication identifier being queried.
        now: Optional timestamp override.

    Returns:
        The newest active owner payload, or ``None`` if no active owner exists.
    """
    active = [job for job in jobs if is_deduplication_active(job, deduplication_id, now=now)]
    if not active:
        return None
    return max(active, key=lambda job: float(job.get("created_at") or 0.0))


def can_replace_deduplicated_job(job: Mapping[str, Any]) -> bool:
    """
    Return whether BullMQ-style replace semantics may evict the current owner.

    Replace mode is intended for delayed jobs that have not started executing
    yet. AsyncMQ therefore only replaces a deduplicated owner while the job is
    still visible as delayed.

    Args:
        job: The stored job payload being considered for replacement.

    Returns:
        ``True`` if the job is still delayed and may be replaced.
    """
    return infer_job_type(dict(job)) == "delayed"


def with_cleared_deduplication(job: Mapping[str, Any]) -> dict[str, Any]:
    """
    Return a copy of a job payload without deduplication metadata.

    Args:
        job: The stored job payload.

    Returns:
        A mutable copy with deduplication metadata removed.
    """
    updated = dict(job)
    updated.pop("deduplication", None)
    return updated


def with_updated_deduplication_window(
    job: Mapping[str, Any],
    *,
    ttl: float | None,
    keep_existing_expiry: bool,
) -> dict[str, Any]:
    """
    Return a copy of a job payload with a refreshed deduplication expiration.

    Args:
        job: The stored job payload to update.
        ttl: The TTL window in seconds. ``None`` keeps the job in simple mode.
        keep_existing_expiry: When ``True``, preserve the existing expiration
            timestamp instead of resetting it to ``now + ttl``.

    Returns:
        A mutable copy containing the updated deduplication payload.
    """
    updated = dict(job)
    deduplication = dict(extract_deduplication(job) or {})
    if not deduplication:
        return updated

    if ttl is None:
        deduplication["ttl"] = None
        deduplication["expires_at"] = None
    elif keep_existing_expiry and isinstance(deduplication.get("expires_at"), (int, float)):
        deduplication["ttl"] = float(ttl)
    else:
        deduplication["ttl"] = float(ttl)
        deduplication["expires_at"] = time.time() + float(ttl)

    updated["deduplication"] = deduplication
    return updated
