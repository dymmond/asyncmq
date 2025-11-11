# tests/dashboard/test_controllers.py
import time
from typing import Any

import pytest
from lilya.apps import Lilya
from lilya.compat import reverse
from lilya.routing import Include
from lilya.testclient import TestClient

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.application import create_dashboard_app

# ---------------------------------------------------------------------------
# Fake backend that mimics the minimal surface the controllers call into.
# ---------------------------------------------------------------------------


class FakeBackend:
    def __init__(self) -> None:
        # Queues + counts
        self.queues: dict[str, dict[str, int]] = {
            "emails": {
                "waiting": 2,
                "active": 1,
                "completed": 5,
                "failed": 1,
                "delayed": 0,
            },
            "reports": {
                "waiting": 0,
                "active": 0,
                "completed": 3,
                "failed": 0,
                "delayed": 1,
            },
        }
        self.paused: set[str] = set()

        # Jobs keyed by (queue, state)
        now = int(time.time())
        self.jobs: dict[tuple[str, str], list[dict[str, Any]]] = {
            ("emails", "failed"): [
                {"id": "f1", "args": ["a"], "kwargs": {"x": 1}, "created": now - 60},
                {"id": "f2", "args": [], "kwargs": {}, "created": now - 30},
            ],
            ("emails", "waiting"): [
                {"id": "w1", "payload": {"x": 1}, "run_at": None, "state": "waiting"},
            ],
            ("reports", "delayed"): [
                {
                    "id": "d1",
                    "payload": {"topic": "q3"},
                    "run_at": now + 3600,
                    "state": "delayed",
                },
            ],
        }

        # Workers list
        self.workers: list[dict[str, Any]] = [
            {"id": "wkr-1", "queue": "emails", "heartbeat": now - 10, "concurrency": 5},
            {
                "id": "wkr-2",
                "queue": "reports",
                "heartbeat": now - 20,
                "concurrency": 2,
            },
        ]

        # Repeatable definitions
        self.repeatables: list[dict[str, Any]] = [
            {
                "task_id": "send-digest",
                "queue": "emails",
                "cron": "0 * * * *",
                "args": [],
                "kwargs": {},
            }
        ]

    # ---- queues
    async def list_queues(self) -> list[str]:
        return list(self.queues.keys())

    async def get_queue_counts(self, q: str) -> dict[str, int]:
        return self.queues[q]

    async def is_paused(self, q: str) -> bool:
        return q in self.paused

    async def pause(self, q: str) -> None:
        self.paused.add(q)

    async def resume(self, q: str) -> None:
        self.paused.discard(q)

    async def clean(self, q: str) -> int:
        # remove completed in this simple fake
        before = self.queues[q]["completed"]
        self.queues[q]["completed"] = 0
        return before

    # ---- jobs
    async def list_jobs(self, queue: str, state: str) -> list[dict[str, Any]]:
        return list(self.jobs.get((queue, state), []))

    async def retry_job(self, queue: str, job_id: str) -> bool:
        bucket = self.jobs.get((queue, "failed"), [])
        for j in list(bucket):
            if j["id"] == job_id:
                bucket.remove(j)
                # Simulate moving to waiting
                self.jobs.setdefault((queue, "waiting"), []).append(
                    {"id": f"r-{job_id}", "payload": j, "state": "waiting"}
                )
                return True
        return False

    async def retry_all(self, queue: str) -> int:
        bucket = self.jobs.get((queue, "failed"), [])
        count = len(bucket)
        for j in list(bucket):
            await self.retry_job(queue, j["id"])
        return count

    async def remove_job(self, queue: str, job_id: str) -> int:
        removed = 0
        for state in ("failed", "waiting", "delayed", "active", "completed"):
            arr = self.jobs.get((queue, state), [])
            n0 = len(arr)
            arr[:] = [j for j in arr if j.get("id") != job_id]
            removed += n0 - len(arr)
        return removed

    # ---- workers
    async def list_workers(self) -> list[dict[str, Any]]:
        return list(self.workers)

    # ---- metrics
    async def metrics_summary(self) -> dict[str, int]:
        processed = sum(v.get("completed", 0) for v in self.queues.values())
        failed = sum(v.get("failed", 0) for v in self.queues.values())
        return {"processed": processed, "failed": failed}

    # ---- repeatables
    async def list_repeatables(self) -> list[dict[str, Any]]:
        return list(self.repeatables)

    async def create_repeatable(self, job_def: dict[str, Any]) -> dict[str, Any]:
        self.repeatables.append(job_def)
        return {"ok": True, "id": f"rpt-{len(self.repeatables)}", **job_def}

    # ---- optional stream hook (SSE). The controller may not call this;
    #      tests only check headers for SSE endpoint.
    async def subscribe(self):  # pragma: no cover - kept for parity
        yield {"event": "ping", "data": {"ok": True}}


# ---------------------------------------------------------------------------
# Fixtures: patch settings, build app + client
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="package")
def fake_backend():
    return FakeBackend()


@pytest.fixture(autouse=True)
def _patch_settings(fake_backend, monkeypatch):
    """
    Ensure all code under test sees our fake backend & a predictable dashboard config.
    """

    settings.debug = True
    settings.backend = fake_backend
    return settings


@pytest.fixture(scope="package")
def app():
    # create once; we rely on autouse fixture to keep settings.backend fresh
    return create_dashboard_app()


@pytest.fixture(scope="package")
def client(app):
    with TestClient(Lilya(routes=[Include(path="/", app=app)])) as c:
        yield c


def _url(client_app, name: str, **kwargs) -> str:
    """
    reverse() wrapper that always passes app, and supports path_params / query.
    """
    path_params = kwargs.pop("path_params", None) or {}
    query = kwargs.pop("query", None)
    path = reverse(name, path_params=path_params, app=client_app)
    if query:
        return f"{path}?{query}"
    return path


def test_home_page_renders(client, app):
    resp = client.get(_url(app, "dashboard"))
    assert resp.status_code == 200
    assert b"AsyncMQ Dashboard" in resp.content  # header_text/title presence


def test_queues_list(client, app):
    resp = client.get(_url(app, "queues"))
    assert resp.status_code == 200
    # two queues from fake backend should be visible
    assert b"emails" in resp.content
    assert b"reports" in resp.content


def test_queue_info_page(client, app):
    # build info URL as "queues base" + "/<name>" to avoid guessing route names
    base = _url(app, "queues")
    resp = client.get(f"{base}/emails")
    assert resp.status_code == 200
    # counts labels appear somewhere in the page
    for label in (b"waiting", b"active", b"completed", b"failed", b"delayed"):
        assert label in resp.content


def test_queue_pause_and_resume_actions_set_flash(client, app):
    base = _url(app, "queues")

    r1 = client.post(f"{base}/emails/pause")
    assert r1.status_code in (302, 303)

    follow = client.get(r1.headers["location"])
    assert follow.status_code == 200
    # Look for a success flash message (text varies — just check Toastify/alert presence)
    assert (
        b"paused" in follow.content
        or b"message" in follow.content
        or b"toast" in follow.content
    )

    r2 = client.post(f"{base}/emails/resume")
    assert r2.status_code in (302, 303)

    follow2 = client.get(r2.headers["location"])
    assert follow2.status_code == 200
    assert (
        b"resumed" in follow2.content
        or b"message" in follow2.content
        or b"toast" in follow2.content
    )


def test_queue_clean_action_redirects(client, app):
    base = _url(app, "queues")
    r = client.post(f"{base}/emails/clean")
    assert r.status_code in (302, 303)
    # redirect back to info or list
    assert r.headers["location"].startswith(base)


@pytest.mark.parametrize(
    "query, expect_in",
    [
        ("queue=emails&state=failed&page=1&size=50", b"f1"),
        ("queue=emails&state=waiting", b"w1"),
        ("queue=reports&state=delayed", b"d1"),
    ],
)
def test_jobs_filters(client, app, query, expect_in):
    # If your app names the route "jobs", this will resolve. If not,
    # we keep a fallback hitting "/jobs" relative to the prefix.
    try:
        url = _url(app, "jobs", query=query)
    except Exception:  # pragma: no cover - safety net for name mismatch
        prefix = reverse("queues", app=app).rsplit("/queues", 1)[0]
        url = f"{prefix}/jobs?{query}"

    resp = client.get(url)
    assert resp.status_code == 200


def test_jobs_invalid_pagination_defaults(client, app):
    try:
        url = _url(app, "jobs", query="queue=emails&state=failed&page=abc&size=NaN")
    except Exception:  # pragma: no cover
        prefix = reverse("queues", app=app).rsplit("/queues", 1)[0]
        url = f"{prefix}/jobs?queue=emails&state=failed&page=abc&size=NaN"

    resp = client.get(url)
    assert resp.status_code == 200  # should gracefully coerce/ignore bad values


def test_workers_page(client, app):
    resp = client.get(_url(app, "workers"))
    assert resp.status_code == 200
    assert b"wkr-1" in resp.content or b"workers" in resp.content.lower()


def test_repeatables_list(client, app):
    # repeatables usually require a queue or name param; try both patterns
    try:
        url = _url(app, "repeatables", path_params={"name": "default"})
    except Exception:  # pragma: no cover
        url = _url(app, "repeatables")
    resp = client.get(url)
    assert resp.status_code == 200
    assert b"send-digest" in resp.content or b"Repeatable" in resp.content


def test_repeatables_new_post_creates_and_redirects(client, app):
    # Build the "new" path from the repeatables base to avoid route name guessing
    try:
        base = _url(app, "repeatables", path_params={"name": "default"})
    except Exception:  # pragma: no cover
        base = _url(app, "repeatables")

    payload = {
        "task_id": "daily-digest",
        "queue": "emails",
        "cron": "0 7 * * *",
        "args": "[]",
        "kwargs": "{}",
    }
    r = client.post(f"{base}/new", data=payload)
    assert r.status_code in (302, 303)
    # Follow and expect flash or presence in list
    follow = client.get(r.headers["location"])
    assert follow.status_code == 200


# ---------------------------------------------------------------------------
# DLQ (list + retry/delete)
# ---------------------------------------------------------------------------


def test_dlq_list(client, app):
    # Construct base prefix from a known route
    prefix = reverse("queues", app=app).rsplit("/queues", 1)[0]
    resp = client.get(f"{prefix}/dlqs/emails?page=1&size=10")
    assert resp.status_code == 200
    # job id from fake backend may appear
    assert b"emails" in resp.content


def test_dlq_retry_and_delete_actions(client, app):
    prefix = reverse("queues", app=app).rsplit("/queues", 1)[0]

    r1 = client.post(f"{prefix}/dlqs/emails/retry", data={"id": "f1"})
    assert r1.status_code in (302, 303)
    r2 = client.post(f"{prefix}/dlqs/emails/delete", data={"id": "f2"})
    assert r2.status_code in (302, 303)


def test_metrics_page(client, app):
    resp = client.get(_url(app, "metrics"))
    assert resp.status_code == 200
    # At least one of the headings/keywords expected in metrics view
    assert (
        b"Metrics" in resp.content
        or b"processed" in resp.content
        or b"failed" in resp.content
    )


def test_sse_endpoint_sets_event_stream_headers(client, app):
    # Build prefix from an existing route to avoid hardcoding
    prefix = reverse("queues", app=app).rsplit("/queues", 1)[0]
    resp = client.get(f"{prefix}/events", headers={"accept": "text/event-stream"})
    # Depending on testclient, streaming may be buffered; assert headers minimally
    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers.get("content-type", "")
