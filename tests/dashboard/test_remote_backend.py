from __future__ import annotations

import time
from typing import Any

import pytest
from lilya.apps import Lilya
from lilya.routing import Include
from lilya.testclient import TestClient

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.remote import (
    RemoteBackendClient,
    RemoteBackendError,
    create_remote_backend_app,
)
from asyncmq.core.inspection import JobInspectionPage, inspect_job_page

REMOTE_PREFIX = "/asyncmq-remote"
REMOTE_TOKEN = "test-remote-token"


class WorkerBackend:
    """
    Backend-shaped worker fixture for the remote dashboard tests.

    It models the operations the dashboard needs without requiring Redis,
    PostgreSQL, MongoDB, RabbitMQ, or an exposed network port.
    """

    def __init__(self) -> None:
        """
        Initialize deterministic queue, job, worker, and repeatable state.

        The fixture keeps mutations in memory so assertions can prove that the
        dashboard acts on the worker-side backend, not local dashboard state.
        """
        now = int(time.time())
        self._queues = {"emails": {"waiting": 1, "failed": 1, "active": 0, "delayed": 0, "completed": 0}}
        self._paused: set[str] = set()
        self._jobs: dict[tuple[str, str], list[dict[str, Any]]] = {
            ("emails", "waiting"): [
                {
                    "id": "w1",
                    "task_id": "send-welcome",
                    "payload": {"message": "hello"},
                    "status": "waiting",
                    "created_at": now - 30,
                }
            ],
            ("emails", "failed"): [
                {
                    "id": "f1",
                    "task_id": "send-receipt",
                    "kwargs": {"customer": "one"},
                    "status": "failed",
                    "last_error": "RuntimeError: receipt failed",
                    "created_at": now - 10,
                }
            ],
        }
        self._workers = [{"id": "worker-1", "queue": "emails", "concurrency": 4, "heartbeat": now}]
        self._repeatables = [
            {
                "job_def": {"task_id": "send-digest", "queue": "emails", "every": 60},
                "next_run": now + 60,
                "paused": False,
            }
        ]

    async def health_check(self) -> None:
        """
        Report that the worker-side backend can be reached.

        The remote app calls this through the same method shape as real
        backends expose for readiness checks.
        """
        return None

    async def list_queues(self) -> list[str]:
        """
        Return known queue names.

        Dashboard queue, overview, metrics, and event routes use this backend
        method as their entry point.
        """
        return list(self._queues)

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Return queue counters for a single queue.

        Unknown queues return zero counters so the dashboard can render empty
        states without special-case test behavior.
        """
        return dict(self._queues.get(queue_name, {}))

    async def list_jobs(self, queue: str, state: str) -> list[dict[str, Any]]:
        """
        Return jobs for one queue and state bucket.

        The remote client also exposes this as the fallback inspection path.
        """
        return [dict(job) for job in self._jobs.get((queue, state), [])]

    async def inspect_jobs(
        self,
        queue_name: str,
        state: str,
        *,
        page: int = 1,
        size: int = 20,
        q: str = "",
        task: str = "",
        job_id: str = "",
        sort: str = "newest",
    ) -> JobInspectionPage:
        """
        Return a paged inspection result for the dashboard job lists.

        This verifies that the remote client reconstructs AsyncMQ's
        ``JobInspectionPage`` contract instead of returning raw dictionaries.
        """
        return inspect_job_page(
            self._jobs.get((queue_name, state), []),
            page=page,
            size=size,
            q=q,
            task=task,
            job_id=job_id,
            sort=sort,
        )

    async def get_job(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        """
        Return a single job from any state bucket.

        Job detail controllers prefer this point lookup when it is available.
        """
        for (queue, _state), jobs in self._jobs.items():
            if queue != queue_name:
                continue
            for job in jobs:
                if job.get("id") == job_id:
                    return dict(job)
        return None

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Return the state bucket for a single job.

        This mirrors the dashboard detail contract used by real backends.
        """
        for (queue, state), jobs in self._jobs.items():
            if queue == queue_name and any(job.get("id") == job_id for job in jobs):
                return state
        return None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any:
        """
        Return a stored job result when one exists.

        The fixture does not store results, but the method proves that the
        remote client supports the dashboard detail contract.
        """
        job = await self.get_job(queue_name, job_id)
        return job.get("result") if job else None

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        """
        Move a failed job back to waiting state.

        The tests assert this mutation through the worker fixture to prove the
        dashboard is controlling the remote backend.
        """
        failed = self._jobs.get((queue_name, "failed"), [])
        for job in list(failed):
            if job.get("id") == job_id:
                failed.remove(job)
                retried = dict(job)
                retried["status"] = "waiting"
                self._jobs.setdefault((queue_name, "waiting"), []).append(retried)
                return True
        return False

    async def remove_job(self, queue_name: str, job_id: str) -> int:
        """
        Remove a job from all state buckets.

        The integer return shape matches common backend implementations.
        """
        removed = 0
        for (queue, _state), jobs in self._jobs.items():
            if queue != queue_name:
                continue
            before = len(jobs)
            jobs[:] = [job for job in jobs if job.get("id") != job_id]
            removed += before - len(jobs)
        return removed

    async def cancel_job(self, queue_name: str, job_id: str) -> bool:
        """
        Cancel a job by removing it from backend state.

        This is enough for the dashboard action contract under test.
        """
        return bool(await self.remove_job(queue_name, job_id))

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Return whether a queue is currently paused.

        Queue list and detail pages call this method when present.
        """
        return queue_name in self._paused

    async def pause_queue(self, queue_name: str) -> None:
        """
        Mark a queue as paused.

        The dashboard queue form uses this method for operator pause actions.
        """
        self._paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Mark a queue as resumed.

        The dashboard queue form uses this method for operator resume actions.
        """
        self._paused.discard(queue_name)

    async def list_workers(self) -> list[dict[str, Any]]:
        """
        Return active worker heartbeat records.

        The dashboard workers page accepts dictionary records after remote JSON
        normalization.
        """
        return [dict(worker) for worker in self._workers]

    async def list_repeatables(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Return repeatable definitions for a queue.

        This proves dataclass-like and dictionary-like repeatable records can
        cross the remote boundary as JSON dictionaries.
        """
        return [dict(item) for item in self._repeatables if item["job_def"].get("queue") == queue_name]

    async def upsert_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Store a repeatable definition and return the next run timestamp.

        Queue helper parity relies on this method existing on the remote
        backend client even when the dashboard route does not exercise it.
        """
        next_run = time.time() + 60
        self._repeatables.append({"job_def": {"queue": queue_name, **job_def}, "next_run": next_run, "paused": False})
        return next_run

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Pause a repeatable definition.

        The action identifies repeatables by queue and task identifier.
        """
        for item in self._repeatables:
            if item["job_def"].get("queue") == queue_name and item["job_def"].get("task_id") == job_def.get("task_id"):
                item["paused"] = True

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Resume a repeatable definition.

        The return value mirrors backends that report the next scheduled run.
        """
        next_run = time.time() + 60
        for item in self._repeatables:
            if item["job_def"].get("queue") == queue_name and item["job_def"].get("task_id") == job_def.get("task_id"):
                item["paused"] = False
                item["next_run"] = next_run
        return next_run

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any] | str) -> None:
        """
        Remove a repeatable definition.

        The fixture accepts the full action definition shape used by the
        dashboard repeatables controller.
        """
        task_id = job_def if isinstance(job_def, str) else job_def.get("task_id")
        self._repeatables = [
            item
            for item in self._repeatables
            if not (item["job_def"].get("queue") == queue_name and item["job_def"].get("task_id") == task_id)
        ]


@pytest.fixture()
def anyio_backend() -> str:
    """
    Run async remote-client tests on asyncio.

    The project-level test environment also supports Trio, but these tests use
    Lilya's synchronous TestClient as an in-process HTTP transport.
    """
    return "asyncio"


@pytest.fixture()
def worker_backend() -> WorkerBackend:
    """
    Provide one fresh worker-side backend per test.

    Fresh state makes remote dashboard mutations straightforward to assert.
    """
    return WorkerBackend()


@pytest.fixture()
def worker_client(worker_backend: WorkerBackend) -> TestClient:
    """
    Serve the remote backend app inside the test process.

    The service is mounted under ``/asyncmq-remote`` to match the documented
    worker-control deployment shape.
    """
    app = Lilya(routes=[Include(path=REMOTE_PREFIX, app=create_remote_backend_app(worker_backend, token=REMOTE_TOKEN))])
    with TestClient(app) as client:
        yield client


@pytest.fixture()
def remote_backend(worker_client: TestClient) -> RemoteBackendClient:
    """
    Provide a dashboard-side remote backend client.

    The transport sends requests to the worker TestClient, which simulates an
    internal-only container network without opening any host port.
    """

    def transport(
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        headers: dict[str, str],
        query: dict[str, str] | None,
    ) -> tuple[int, Any]:
        """
        Send a remote client request through the in-process worker app.

        The configured base URL is intentionally not reachable from the test
        host; only the supplied private transport can reach the worker app.
        """
        response = worker_client.request(
            method,
            f"{REMOTE_PREFIX}{path}",
            json=payload,
            headers=headers,
            params=query or {},
        )
        return response.status_code, response.json()

    return RemoteBackendClient(
        "http://asyncmq-worker:8001/asyncmq-remote",
        token=REMOTE_TOKEN,
        transport=transport,
    )


@pytest.mark.anyio
async def test_remote_backend_client_controls_worker_backend(
    remote_backend: RemoteBackendClient,
    worker_backend: WorkerBackend,
) -> None:
    """
    Prove direct remote backend calls mutate worker-owned state.

    This covers the core deployment split where the dashboard process has a
    remote backend client and the worker service owns the real backend.
    """
    await remote_backend.health_check()

    assert await remote_backend.list_queues() == ["emails"]
    assert await remote_backend.is_queue_paused("emails") is False

    await remote_backend.pause_queue("emails")
    assert await worker_backend.is_queue_paused("emails") is True

    inspected = await remote_backend.inspect_jobs("emails", "failed", page=1, size=20)
    assert isinstance(inspected, JobInspectionPage)
    assert inspected.jobs[0]["id"] == "f1"

    assert await remote_backend.retry_job("emails", "f1") is True
    assert await worker_backend.get_job_state("emails", "f1") == "waiting"

    repeatables = await remote_backend.list_repeatables("emails")
    assert repeatables[0]["job_def"]["task_id"] == "send-digest"


def test_dashboard_uses_remote_backend_client(
    remote_backend: RemoteBackendClient,
    worker_backend: WorkerBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Prove AsyncMQAdmin works when settings.backend is a remote client.

    The real dashboard controllers are mounted and exercised through HTTP so
    this is not just a remote-client unit test.
    """
    monkeypatch.setattr(settings, "backend", remote_backend)
    monkeypatch.setattr(settings, "debug", True)
    monkeypatch.setattr(settings, "heartbeat_ttl", 30)
    monkeypatch.setattr(settings, "secret_key", "remote-dashboard-test-secret")

    app = Lilya()
    admin = AsyncMQAdmin(
        enable_login=False,
        url_prefix="/asyncmq",
        include_security=False,
        include_cors=False,
    )
    admin.include_in(app)

    with TestClient(app, follow_redirects=True) as client:
        queues = client.get("/asyncmq/queues")
        assert queues.status_code == 200
        assert "emails" in queues.text

        pause = client.post("/asyncmq/queues/emails", data={"action": "pause"})
        assert 303 in [item.status_code for item in pause.history]
        assert pause.status_code == 200
        assert "paused" in pause.text
        assert "emails" in pause.text

        jobs = client.get("/asyncmq/queues/emails/jobs?state=failed")
        assert jobs.status_code == 200
        assert "f1" in jobs.text

        action = client.post(
            "/asyncmq/queues/emails/jobs",
            data={"action": "retry", "job_id": "f1", "state": "failed", "size": "20"},
        )
        assert action.status_code == 200
        assert "f1" in action.text

        workers = client.get("/asyncmq/workers")
        assert workers.status_code == 200
        assert "worker-1" in workers.text

    assert worker_backend._paused == {"emails"}
    assert worker_backend._jobs[("emails", "failed")] == []


@pytest.mark.anyio
async def test_remote_backend_rejects_invalid_token(worker_client: TestClient) -> None:
    """
    Prove the worker-side remote endpoint rejects unauthorized callers.

    This verifies the remote option remains suitable for private control-plane
    use even when the worker service is reachable inside a cluster network.
    """

    def transport(
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        headers: dict[str, str],
        query: dict[str, str] | None,
    ) -> tuple[int, Any]:
        """
        Send a request with caller-supplied headers to the worker app.

        The fixture uses the same private transport as authorized clients so
        only the token value differs.
        """
        response = worker_client.request(
            method,
            f"{REMOTE_PREFIX}{path}",
            json=payload,
            headers=headers,
            params=query or {},
        )
        return response.status_code, response.json()

    remote_backend = RemoteBackendClient(
        "http://asyncmq-worker:8001/asyncmq-remote",
        token="wrong-token",
        transport=transport,
    )

    with pytest.raises(RemoteBackendError, match="Invalid AsyncMQ remote backend token"):
        await remote_backend.list_queues()
