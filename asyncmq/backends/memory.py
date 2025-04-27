import time
from typing import Any, Dict, List, Optional, Set, Tuple

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.event import event_emitter


class InMemoryBackend(BaseBackend):
    def __init__(self):
        self.queues: Dict[str, List[dict]] = {}
        self.dlqs: Dict[str, List[dict]] = {}
        self.delayed: Dict[str, List[Tuple[float, dict]]] = {}
        self.job_states: Dict[tuple, str] = {}
        self.job_results: Dict[tuple, Any] = {}
        self.job_progress: Dict[tuple, float] = {}
        self.deps_pending: Dict[tuple, Set[str]] = {}
        self.deps_children: Dict[tuple, Set[str]] = {}
        self.paused: Set[str] = set()
        self.lock = anyio.Lock()

    async def enqueue(self, queue_name: str, payload: dict):
        async with self.lock:
            queue = self.queues.setdefault(queue_name, [])
            queue.append(payload)
            queue.sort(key=lambda job: job.get("priority", 5))  # Lower number = higher priority
            self.job_states[(queue_name, payload["id"])] = "waiting"

    async def dequeue(self, queue_name: str) -> Optional[dict]:
        async with self.lock:
            queue = self.queues.get(queue_name, [])
            if queue:
                return queue.pop(0)
            return None

    async def move_to_dlq(self, queue_name: str, payload: dict):
        async with self.lock:
            dlq = self.dlqs.setdefault(queue_name, [])
            dlq.append(payload)
            self.job_states[(queue_name, payload["id"])] = "failed"

    async def ack(self, queue_name: str, job_id: str):
        ...  # No-op

    async def enqueue_delayed(self, queue_name: str, payload: dict, run_at: float):
        async with self.lock:
            delayed_list = self.delayed.setdefault(queue_name, [])
            delayed_list.append((run_at, payload))
            self.job_states[(queue_name, payload["id"])] = "delayed"

    async def get_due_delayed(self, queue_name: str) -> List[dict]:
        async with self.lock:
            now = time.time()
            due_jobs: List[dict] = []
            still: List[Tuple[float, dict]] = []
            for run_at, payload in self.delayed.get(queue_name, []):
                if run_at <= now:
                    due_jobs.append(payload)
                else:
                    still.append((run_at, payload))
            self.delayed[queue_name] = still
            return due_jobs

    async def remove_delayed(self, queue_name: str, job_id: str):
        ...  # Already handled in get_due_delayed

    async def update_job_state(self, queue_name: str, job_id: str, state: str):
        async with self.lock:
            self.job_states[(queue_name, job_id)] = state

    async def save_job_result(self, queue_name: str, job_id: str, result: Any):
        async with self.lock:
            self.job_results[(queue_name, job_id)] = result

    async def get_job_state(self, queue_name: str, job_id: str) -> Optional[str]:
        return self.job_states.get((queue_name, job_id))

    async def get_job_result(self, queue_name: str, job_id: str) -> Optional[Any]:
        return self.job_results.get((queue_name, job_id))

    async def add_dependencies(self, queue_name: str, job_dict: dict):
        job_id = job_dict['id']
        pend_key = (queue_name, job_id)
        deps = set(job_dict.get('depends_on', []))
        if not deps:
            return
        self.deps_pending[pend_key] = deps
        for parent in deps:
            child_key = (queue_name, parent)
            self.deps_children.setdefault(child_key, set()).add(job_id)

    async def resolve_dependency(self, queue_name: str, parent_id: str):
        child_key = (queue_name, parent_id)
        children = self.deps_children.get(child_key, set())
        for child_id in list(children):
            pend_key = (queue_name, child_id)
            self.deps_pending[pend_key].discard(parent_id)
            if not self.deps_pending[pend_key]:
                raw = self._fetch_job_data(queue_name, child_id)
                if raw:
                    await self.enqueue(queue_name, raw)
                    await event_emitter.emit('job:ready', {'id': child_id})
                del self.deps_pending[pend_key]
        self.deps_children.pop(child_key, None)

    async def pause_queue(self, queue_name: str):
        self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str):
        self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        return queue_name in self.paused

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float):
        self.job_progress[(queue_name, job_id)] = progress

    async def bulk_enqueue(self, queue_name: str, jobs: List[dict]):
        async with self.lock:
            q = self.queues.setdefault(queue_name, [])
            for job in jobs:
                q.append(job)
                self.job_states[(queue_name, job['id'])] = 'waiting'

    async def purge(self, queue_name: str, state: str, older_than: Optional[float] = None):
        to_delete = []
        for (qname, jid), st in list(self.job_states.items()):
            if qname == queue_name and st == state:
                to_delete.append((qname, jid))
        for key in to_delete:
            self.job_states.pop(key, None)
            self.job_results.pop(key, None)
            self.job_progress.pop(key, None)

    async def emit_event(self, event: str, data: dict):
        await event_emitter.emit(event, data)

    async def create_lock(self, key: str, ttl: int):
        """
        Return a backend-agnostic lock (TTL not enforced).
        """
        return anyio.Lock()

    def _fetch_job_data(self, queue_name: str, job_id: str) -> Optional[dict]:
        for job in self.queues.get(queue_name, []):
            if job.get('id') == job_id:
                return job
        return None
