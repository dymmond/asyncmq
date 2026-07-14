from __future__ import annotations

import os
import time
from typing import Any

from lilya.apps import Lilya

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.backends.simple_user import SimpleUsernamePasswordBackend
from asyncmq.contrib.dashboard.admin.protocols import User


class NginxProxyBackend:
    """Small runtime backend used by the Nginx dashboard proof environment."""

    def __init__(self) -> None:
        """Seed deterministic queue, worker, and job state for browser proof."""
        now = int(time.time())
        self.paused: set[str] = set()
        self.jobs: list[dict[str, Any]] = [
            {
                "id": "proxy-job-001",
                "task_id": "send-proxy-proof",
                "status": "failed",
                "kwargs": {"api_token": "nginx-secret-token"},
                "last_error": "RuntimeError: proxy proof failed",
                "error_traceback": (
                    "Traceback (most recent call last):\n"
                    '  File "asyncmq/workers.py", line 418, in process_job\n'
                    "    await task(*job.args, **job.kwargs)\n"
                    '  File "tasks/proxy.py", line 12, in send_proxy_proof\n'
                    '    raise RuntimeError("proxy proof failed")\n'
                    "RuntimeError: proxy proof failed"
                ),
                "created_at": now - 30,
            }
        ]
        self.workers = [
            {
                "id": "proxy-worker-1",
                "status": "online",
                "queues": ["critical-email"],
                "concurrency": 4,
                "current_jobs": 1,
                "reserved_jobs": 0,
                "scheduled_jobs": 0,
                "last_seen": now,
                "heartbeat_at": now,
                "started_at": now - 3600,
            }
        ]

    async def list_queues(self) -> list[str]:
        """Return the queues visible to the proof dashboard."""
        return ["critical-email"]

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """Return deterministic queue counts without unbounded scans."""
        return {"waiting": 3, "active": 1, "delayed": 0, "failed": 1, "completed": 8}

    async def is_paused(self, queue_name: str) -> bool:
        """Return whether the proof queue is paused."""
        return queue_name in self.paused

    async def pause_queue(self, queue_name: str) -> None:
        """Pause a queue for dashboard action proof."""
        self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """Resume a queue for dashboard action proof."""
        self.paused.discard(queue_name)

    async def list_workers(self) -> list[dict[str, Any]]:
        """Return proof worker metadata."""
        return list(self.workers)

    async def list_jobs(self, queue: str, state: str) -> list[dict[str, Any]]:
        """Return proof jobs in the requested state."""
        return [job for job in self.jobs if job.get("status") == state]

    async def get_job(self, queue: str, job_id: str) -> dict[str, Any] | None:
        """Return a proof job by identifier."""
        for job in self.jobs:
            if job["id"] == job_id:
                return dict(job)
        return None

    async def get_job_state(self, queue: str, job_id: str) -> str | None:
        """Return the proof job state by identifier."""
        job = await self.get_job(queue, job_id)
        return str(job["status"]) if job else None

    async def get_job_result(self, queue: str, job_id: str) -> Any | None:
        """Return no result for failed proof jobs."""
        return None


def verify_user(username: str, password: str) -> User | None:
    """Authenticate the fixed proof operator account."""
    if username == "admin" and password == "secret":
        return User(id="admin", name="Admin", is_admin=True)
    return None


settings.backend = NginxProxyBackend()

app = Lilya()
admin = AsyncMQAdmin(
    enable_login=True,
    backend=SimpleUsernamePasswordBackend(verify_user),
    include_session=True,
    include_cors=False,
    url_prefix=os.environ.get("ASYNCMQ_DASHBOARD_PREFIX", "/asyncmq"),
    trusted_proxies=("127.0.0.1", "172.17.0.1", "172.18.0.1"),
)
admin.include_in(app)
