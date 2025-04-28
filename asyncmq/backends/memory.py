import time
from typing import Any, Tuple

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter

_JobKey = Tuple[str, str]

class InMemoryBackend(BaseBackend):
    def __init__(self) -> None:
        self.queues: dict[str, list[dict[str, Any]]] = {}
        self.dlqs: dict[str, list[dict[str, Any]]] = {}
        self.delayed: dict[str, list[tuple[float, dict[str, Any]]]] = {}
        self.job_states: dict[_JobKey, str] = {}
        self.job_results: dict[_JobKey, Any] = {}
        self.job_progress: dict[_JobKey, float] = {}
        self.deps_pending: dict[_JobKey, set[str]] = {}
        self.deps_children: dict[_JobKey, set[str]] = {}
        self.paused: set[str] = set()
        self.lock: anyio.Lock = anyio.Lock()

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
        async with self.lock:
            queue = self.queues.setdefault(queue_name, [])
            queue.append(payload)
            queue.sort(key=lambda job: job.get("priority", 5))
            self.job_states[(queue_name, payload["id"])] = State.WAITING

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        async with self.lock:
            queue = self.queues.get(queue_name, [])
            if queue:
                return queue.pop(0)
            return None

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        async with self.lock:
            dlq = self.dlqs.setdefault(queue_name, [])
            dlq.append(payload)
            self.job_states[(queue_name, payload["id"])] = State.FAILED

    async def ack(self, queue_name: str, job_id: str) -> None:
        pass

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        async with self.lock:
            delayed_list = self.delayed.setdefault(queue_name, [])
            delayed_list.append((run_at, payload))
            self.job_states[(queue_name, payload["id"])] = State.EXPIRED

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        async with self.lock:
            now = time.time()
            due_jobs = []
            still_delayed = []
            for run_at, payload in self.delayed.get(queue_name, []):
                if run_at <= now:
                    due_jobs.append(payload)
                else:
                    still_delayed.append((run_at, payload))
            self.delayed[queue_name] = still_delayed
            return due_jobs

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        pass

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        async with self.lock:
            self.job_states[(queue_name, job_id)] = state

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        async with self.lock:
            self.job_results[(queue_name, job_id)] = result

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        return self.job_states.get((queue_name, job_id))

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        return self.job_results.get((queue_name, job_id))

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        child_job_id = job_dict['id']
        child_pend_key: _JobKey = (queue_name, child_job_id)
        parent_deps: set[str] = set(job_dict.get('depends_on', []))
        if not parent_deps:
            return
        self.deps_pending[child_pend_key] = parent_deps
        for parent_id in parent_deps:
            parent_children_key: _JobKey = (queue_name, parent_id)
            self.deps_children.setdefault(parent_children_key, set()).add(child_job_id)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        parent_children_key: _JobKey = (queue_name, parent_id)
        children: list[str] = list(self.deps_children.get(parent_children_key, set()))
        for child_id in children:
            child_pend_key: _JobKey = (queue_name, child_id)
            if child_pend_key in self.deps_pending:
                self.deps_pending[child_pend_key].discard(parent_id)
                if not self.deps_pending[child_pend_key]:
                    raw: dict[str, Any] | None = self._fetch_job_data(queue_name, child_id)
                    if raw:
                        await self.enqueue(queue_name, raw)
                        await event_emitter.emit('job:ready', {'id': child_id})
                    del self.deps_pending[child_pend_key]
        self.deps_children.pop(parent_children_key, None)

    async def pause_queue(self, queue_name: str) -> None:
        self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        return queue_name in self.paused

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        self.job_progress[(queue_name, job_id)] = progress

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        async with self.lock:
            q = self.queues.setdefault(queue_name, [])
            q.extend(jobs)
            for job in jobs:
                self.job_states[(queue_name, job['id'])] = State.WAITING
            q.sort(key=lambda job: job.get("priority", 5))

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        to_delete: list[_JobKey] = []
        for (qname, jid), st in list(self.job_states.items()):
            if qname == queue_name and st == state:
                to_delete.append((qname, jid))
        for key in to_delete:
            self.job_states.pop(key, None)
            self.job_results.pop(key, None)
            self.job_progress.pop(key, None)

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        await event_emitter.emit(event, data)

    async def create_lock(self, key: str, ttl: int) -> anyio.Lock:
        return anyio.Lock()

    def _fetch_job_data(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        for job in self.queues.get(queue_name, []):
            if job.get('id') == job_id:
                return job
        return None

    async def list_queues(self) -> list[str]:
        async with self.lock:
            return list(self.queues.keys())

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        async with self.lock:
            waiting = len(self.queues.get(queue_name, []))
            delayed = len(self.delayed.get(queue_name, []))
            failed = len(self.dlqs.get(queue_name, []))
            return {
                "waiting": waiting,
                "delayed": delayed,
                "failed": failed,
            }

    async def list_jobs(self, queue_name: str) -> list[dict[str, Any]]:
        async with self.lock:
            jobs = []
            for job in self.queues.get(queue_name, []):
                jobs.append(job)
            for _, job in self.delayed.get(queue_name, []):
                jobs.append(job)
            for job in self.dlqs.get(queue_name, []):
                jobs.append(job)
            return jobs

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        async with self.lock:
            dlq = self.dlqs.get(queue_name, [])
            for job in dlq:
                if job["id"] == job_id:
                    dlq.remove(job)
                    self.queues.setdefault(queue_name, []).append(job)
                    self.job_states[(queue_name, job_id)] = State.WAITING
                    return True
            return False

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        async with self.lock:
            for lst in (self.queues.get(queue_name, []),
                        [job for _, job in self.delayed.get(queue_name, [])],
                        self.dlqs.get(queue_name, [])):
                for job in list(lst):
                    if job["id"] == job_id:
                        try:
                            lst.remove(job)
                        except ValueError:
                            pass
                        self.job_states.pop((queue_name, job_id), None)
                        self.job_results.pop((queue_name, job_id), None)
                        return True
            return False
