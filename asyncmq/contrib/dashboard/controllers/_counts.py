from __future__ import annotations

from typing import Any

JOB_STATES: tuple[str, ...] = ("waiting", "active", "delayed", "failed", "completed")


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


async def get_queue_state_counts(backend: Any, queue_name: str) -> dict[str, int]:
    """
    Return per-state job counts for a queue.

    Uses `queue_stats` when available and backfills missing states with `list_jobs`.
    """
    counts: dict[str, int] = dict.fromkeys(JOB_STATES, 0)
    provided: set[str] = set()

    if hasattr(backend, "queue_stats"):
        try:
            stats = await backend.queue_stats(queue_name)
        except Exception:
            stats = {}
        if isinstance(stats, dict):
            waiting_value = stats.get("waiting")
            if waiting_value is None and "message_count" in stats:
                waiting_value = stats.get("message_count")
            if waiting_value is not None:
                counts["waiting"] = _to_int(waiting_value)
                provided.add("waiting")

            for state in ("active", "delayed", "failed", "completed"):
                if state in stats:
                    counts[state] = _to_int(stats[state])
                    provided.add(state)

    for state in JOB_STATES:
        if state in provided:
            continue
        try:
            jobs = await backend.list_jobs(queue_name, state)
            counts[state] = len(jobs)
        except Exception:
            counts[state] = 0

    return counts


async def aggregate_counts(backend: Any, queues: list[str]) -> dict[str, int]:
    totals: dict[str, int] = dict.fromkeys(JOB_STATES, 0)
    for queue_name in queues:
        counts = await get_queue_state_counts(backend, queue_name)
        for state in JOB_STATES:
            totals[state] += counts[state]
    return totals
