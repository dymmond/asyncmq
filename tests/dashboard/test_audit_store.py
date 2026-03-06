from __future__ import annotations

from asyncmq.contrib.dashboard.audit import (
    clear_audit_events,
    list_audit_events,
    record_audit_event,
)


def test_audit_store_filters_and_limit():
    clear_audit_events()
    record_audit_event(
        request=None,
        action="job.retry",
        source="jobs.single",
        queue="emails",
        job_id="j1",
        details={"attempt": 2},
    )
    record_audit_event(
        request=None,
        action="queue.pause",
        source="queues.detail",
        status="failed",
        queue="reports",
        error="boom",
    )

    retry_rows = list_audit_events(limit=10, action="job.retry")
    assert len(retry_rows) == 1
    assert retry_rows[0]["job_id"] == "j1"

    failed_rows = list_audit_events(limit=10, status="failed")
    assert len(failed_rows) == 1
    assert failed_rows[0]["queue"] == "reports"

    text_rows = list_audit_events(limit=10, q="attempt")
    assert len(text_rows) == 1
    assert text_rows[0]["action"] == "job.retry"

    limited = list_audit_events(limit=1)
    assert len(limited) == 1

    clear_audit_events()
