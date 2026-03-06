from __future__ import annotations

from asyncmq.contrib.dashboard.metrics_history import (
    clear_metrics_history,
    list_metrics_history,
    record_metrics_snapshot,
)


def test_metrics_history_store_records_and_limits_rows():
    clear_metrics_history()
    record_metrics_snapshot(
        metrics={"throughput": 4, "avg_duration": None, "retries": 1, "failures": 1},
        counts={"waiting": 3, "active": 2, "delayed": 0, "completed": 4, "failed": 1},
        total_queues=2,
        total_workers=5,
    )
    record_metrics_snapshot(
        metrics={"throughput": 5, "avg_duration": None, "retries": 2, "failures": 2},
        counts={"waiting": 2, "active": 1, "delayed": 1, "completed": 5, "failed": 2},
        total_queues=2,
        total_workers=4,
    )

    rows = list_metrics_history(limit=10)
    assert len(rows) == 2
    assert rows[0]["throughput"] == 5
    assert rows[1]["throughput"] == 4
    assert "time" in rows[0]

    limited = list_metrics_history(limit=1)
    assert len(limited) == 1
    assert limited[0]["throughput"] == 5

    clear_metrics_history()
