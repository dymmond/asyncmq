from __future__ import annotations

from asyncmq.core.inspection import inspect_job_page


def test_inspect_job_page_filters_sorts_and_bounds_results():
    jobs = [
        {"id": "old", "task_id": "send-email", "created_at": 10, "payload": {"text": "needle"}},
        {"id": "new", "task_id": "send-email", "created_at": 30, "payload": {"text": "needle"}},
        {"id": "skip", "task_id": "other-task", "created_at": 20, "payload": {"text": "needle"}},
    ]

    page = inspect_job_page(jobs, page=1, size=1, q="needle", task="send", sort="newest")

    assert page.total == 2
    assert page.total_pages == 2
    assert page.page == 1
    assert page.size == 1
    assert [job["id"] for job in page.jobs] == ["new"]


def test_inspect_job_page_clamps_page_to_available_results():
    jobs = [{"id": "only", "created_at": 1}]

    page = inspect_job_page(jobs, page=99, size=20, sort="oldest")

    assert page.total == 1
    assert page.total_pages == 1
    assert page.page == 1
    assert [job["id"] for job in page.jobs] == ["only"]
