from __future__ import annotations

import pytest

from asyncmq.contrib.dashboard.controllers._counts import aggregate_counts, get_queue_state_counts

pytestmark = pytest.mark.anyio


class _BackendWithStats:
    def __init__(self) -> None:
        self.list_jobs_calls: list[tuple[str, str]] = []

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        return {"waiting": 3, "delayed": 2, "failed": 1}

    async def list_jobs(self, queue_name: str, state: str) -> list[dict]:
        self.list_jobs_calls.append((queue_name, state))
        if state == "active":
            return [{"id": "a1"}]
        if state == "completed":
            return [{"id": "c1"}, {"id": "c2"}]
        return []


class _BackendWithoutStats:
    async def list_jobs(self, queue_name: str, state: str) -> list[dict]:
        if state == "waiting":
            return [{"id": "w1"}]
        if state == "active":
            return [{"id": "a1"}, {"id": "a2"}]
        if state == "delayed":
            return []
        if state == "failed":
            return [{"id": "f1"}]
        if state == "completed":
            return [{"id": "c1"}]
        return []


async def test_get_queue_state_counts_uses_stats_and_backfills_missing_states():
    backend = _BackendWithStats()
    counts = await get_queue_state_counts(backend, "emails")

    assert counts == {"waiting": 3, "active": 1, "delayed": 2, "failed": 1, "completed": 2}
    assert ("emails", "active") in backend.list_jobs_calls
    assert ("emails", "completed") in backend.list_jobs_calls
    assert ("emails", "waiting") not in backend.list_jobs_calls


async def test_aggregate_counts_falls_back_to_list_jobs_when_stats_are_unavailable():
    backend = _BackendWithoutStats()
    totals = await aggregate_counts(backend, ["queue-a", "queue-b"])

    assert totals == {"waiting": 2, "active": 4, "delayed": 0, "failed": 2, "completed": 2}
