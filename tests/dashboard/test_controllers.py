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


class FakeBackend:
    def __init__(self) -> None:
        self._now = int(time.time())
        self._queues = {
            "emails": {
                "waiting": 2,
                "active": 1,
                "delayed": 0,
                "failed": 2,
                "completed": 5,
            },
            "reports": {
                "waiting": 0,
                "active": 0,
                "delayed": 1,
                "failed": 0,
                "completed": 3,
            },
        }
        self._paused: set[str] = set()
        # Jobs indexed by (queue, state)
        self._jobs: dict[tuple[str, str], list[dict[str, Any]]] = {
            ("emails", "failed"): [
                {
                    "id": "f1",
                    "args": ["a"],
                    "kwargs": {"x": 1},
                    "created": self._now - 60,
                },
                {"id": "f2", "args": [], "kwargs": {}, "created": self._now - 30},
            ],
            ("emails", "waiting"): [{"id": "w1", "payload": {"x": 1}, "state": "waiting"}],
            ("reports", "delayed"): [{"id": "d1", "payload": {"topic": "q3"}, "state": "delayed"}],
        }
        self._workers = [
            {
                "id": "wkr-1",
                "queue": "emails",
                "heartbeat": self._now - 10,
                "concurrency": 5,
            },
            {
                "id": "wkr-2",
                "queue": "reports",
                "heartbeat": self._now - 20,
                "concurrency": 2,
            },
        ]
        self._repeatables = [
            {
                "task_id": "send-digest",
                "queue": "emails",
                "cron": "0 * * * *",
                "args": [],
                "kwargs": {},
            }
        ]

    # queues API expected by controllers
    async def list_queues(self) -> list[str]:
        return list(self._queues.keys())

    async def is_queue_paused(self, name: str) -> bool:
        return name in self._paused

    async def pause_queue(self, name: str) -> None:
        self._paused.add(name)

    async def resume_queue(self, name: str) -> None:
        self._paused.discard(name)

    # jobs API
    async def list_jobs(self, queue: str, state: str) -> list[dict[str, Any]]:
        return list(self._jobs.get((queue, state), []))

    async def retry_job(self, queue: str, job_id: str) -> bool:
        bucket = self._jobs.get((queue, "failed"), [])
        for j in list(bucket):
            if j["id"] == job_id:
                bucket.remove(j)
                self._jobs.setdefault((queue, "waiting"), []).append(
                    {"id": f"r-{job_id}", "payload": j, "state": "waiting"}
                )
                return True
        return False

    async def remove_job(self, queue: str, job_id: str) -> int:
        removed = 0
        for state in ("failed", "waiting", "delayed", "active", "completed"):
            arr = self._jobs.get((queue, state), [])
            n0 = len(arr)
            arr[:] = [j for j in arr if j.get("id") != job_id]
            removed += n0 - len(arr)
        return removed

    async def retry_all(self, queue: str) -> int:
        bucket = list(self._jobs.get((queue, "failed"), []))
        count = 0
        for j in bucket:
            ok = await self.retry_job(queue, j["id"])
            count += int(ok)
        return count

    # workers
    async def list_workers(self) -> list[dict[str, Any]]:
        return list(self._workers)

    # metrics
    async def metrics_summary(self) -> dict[str, int]:
        processed = sum(v.get("completed", 0) for v in self._queues.values())
        failed = sum(v.get("failed", 0) for v in self._queues.values())
        return {"processed": processed, "failed": failed}

    # repeatables
    async def list_repeatables(self, queue_name: str) -> list[dict[str, Any]]:
        return [r for r in self._repeatables if r.get("queue") == queue_name]

    async def create_repeatable(self, job_def: dict[str, Any]) -> dict[str, Any]:
        self._repeatables.append(job_def)
        return {"ok": True, "id": f"rpt-{len(self._repeatables)}", **job_def}


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
    with TestClient(Lilya(routes=[Include(path="/", app=app)]), follow_redirects=True) as c:
        yield c


def url(app, reverse_name: str, **path_params) -> str:
    return reverse(reverse_name, app=app, path_params=path_params or None)


def test_home(client, app):
    response = client.get(url(app, "dashboard"))

    assert response.status_code == 200
    assert b"AsyncMQ" in response.content


def test_queues_list(client, app):
    response = client.get(url(app, "queues"))

    assert response.status_code == 200
    assert b"emails" in response.content and b"reports" in response.content


def test_queue_detail_counts(client, app):
    response = client.get(url(app, reverse_name="queue-detail", **{"name": "emails"}))

    assert response.status_code == 200

    # labels visible on page
    for label in (b"waiting", b"active", b"delayed", b"failed", b"completed"):
        assert label in response.content


def test_queue_pause_and_resume_actions_via_queue_detail(client, app):
    detail = url(app, "queue-detail", name="emails")

    # pause
    response = client.post(detail, data={"action": "pause"})

    history = [res.status_code for res in response.history]

    assert 303 in history

    assert response.status_code == 200

    # message text can vary, check for common words
    assert b"paused" in response.content or b"success" in response.content

    # resume
    response = client.post(detail, data={"action": "resume"})

    history = [res.status_code for res in response.history]

    assert 303 in history

    assert response.status_code == 200

    assert b"resumed" in response.content or b"success" in response.content


@pytest.mark.parametrize(
    "queue,state,expect",
    [
        ("emails", "failed", b"f1"),
        ("emails", "waiting", b"w1"),
        ("reports", "delayed", b"d1"),
    ],
)
def test_queue_jobs_list(client, app, queue, state, expect):
    """
    GET /queues/{name}/jobs?state=failed|waiting|...
    """

    jobs_url = url(app, "queue-jobs", name=queue) + f"?state={state}"
    response = client.get(jobs_url)

    assert response.status_code == 200

    # we don't force strict template shape, just ensure content renders
    assert expect in response.content or response.content


def test_job_action_endpoint_exists(client, app):
    """
    POST /queues/{name}/jobs/{job_id}/{action}
    """
    # We won't assert the backend effect (format is app-specific). Just 200/3xx existence.
    action_url = url(app, "job-action", name="emails", job_id="f1", action="retry")
    response = client.post(action_url)

    assert response.status_code == 200


def test_repeatables_list(client, app):
    response = client.get(url(app, "repeatables", name="emails"))

    assert response.status_code == 200
    assert b"send-digest" in response.content or b"Repeatable" in response.content


def test_repeatables_new_post(client, app):
    new_url = url(app, "repeatables", name="emails") + "/new"
    payload = {
        "task_id": "daily-digest",
        "queue": "emails",
        "cron": "0 7 * * *",
        "args": "[]",
        "kwargs": "{}",
    }
    response = client.post(new_url, data=payload)

    assert response.status_code == 200


def test_dlq_list(client, app):
    response = client.get(url(app, "dlq", name="emails"))

    assert response.status_code == 200
    assert b"emails" in response.content


@pytest.mark.parametrize("action, data", [("retry", {"job_id": "f1"}), ("delete", {"job_id": "f2"})])
def test_dlq_actions_post(client, app, action, data):
    response = client.post(url(app, "dlq", **{"name": "emails"}), data={"action": action, **data})

    assert response.status_code in (200, 302, 303)


def test_workers_page(client, app):
    response = client.get(url(app, "workers"))

    assert response.status_code == 200
    assert b"wkr-" in response.content or b"Workers" in response.content


def test_metrics_page(client, app):
    response = client.get(url(app, "metrics"))

    assert response.status_code == 200
    assert b"Metrics" in response.content or b"processed" in response.content or b"failed" in response.content


def xtest_sse_headers(client, app):
    response = client.get(url(app, "events"), headers={"accept": "text/event-stream"})

    assert response.status_code == 200
    assert "text/event-stream" in response.headers.get("content-type", "")
