try:
    import motor # noqa
except ImportError:
    raise ImportError("Please install motor: `pip install motor`") from None

import time
from typing import Any, Dict, List, Optional, Set

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.stores.mongodb import MongoDBStore


class MongoDBBackend(BaseBackend):
    def __init__(self, mongo_url: str | None = None, database: str = None) -> None:
        mongo_url = mongo_url or settings.asyncmq_mongodb_backend_url
        database = database or settings.asyncmq_mongodb_database_name

        self.store = MongoDBStore(mongo_url, database)
        self.queues: Dict[str, List[Dict[str, Any]]] = {}
        self.delayed: Dict[str, List[tuple[float, Dict[str, Any]]]] = {}
        self.paused: Set[str] = set()
        self.lock = anyio.Lock()

    async def enqueue(self, queue_name: str, payload: Dict[str, Any]) -> None:
        async with self.lock:
            self.queues.setdefault(queue_name, []).append(payload)
            await self.store.save(queue_name, payload["id"], {**payload, "status": State.WAITING})

    async def dequeue(self, queue_name: str) -> Optional[Dict[str, Any]]:
        async with self.lock:
            queue = self.queues.get(queue_name, [])
            if queue:
                job = queue.pop(0)
                job["status"] = State.ACTIVE
                await self.store.save(queue_name, job["id"], job)
                return job
            return None

    async def move_to_dlq(self, queue_name: str, payload: Dict[str, Any]) -> None:
        async with self.lock:
            payload["status"] = State.FAILED
            await self.store.save(queue_name, payload["id"], payload)

    async def ack(self, queue_name: str, job_id: str) -> None:
        # Nothing required here; handled in update_job_state
        pass

    async def enqueue_delayed(self, queue_name: str, payload: Dict[str, Any], run_at: float) -> None:
        async with self.lock:
            self.delayed.setdefault(queue_name, []).append((run_at, payload))
            await self.store.save(queue_name, payload["id"], {**payload, "status": State.EXPIRED})

    async def get_due_delayed(self, queue_name: str) -> List[Dict[str, Any]]:
        async with self.lock:
            now = time.time()
            due = []
            remaining = []
            for run_at, job in self.delayed.get(queue_name, []):
                if run_at <= now:
                    due.append(job)
                else:
                    remaining.append((run_at, job))
            self.delayed[queue_name] = remaining
            return due

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        async with self.lock:
            self.delayed[queue_name] = [
                (ts, job) for ts, job in self.delayed.get(queue_name, []) if job.get("id") != job_id
            ]

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        job = await self.store.load(queue_name, job_id)
        if job:
            job["status"] = state
            await self.store.save(queue_name, job_id, job)

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        job = await self.store.load(queue_name, job_id)
        if job:
            job["result"] = result
            await self.store.save(queue_name, job_id, job)

    async def get_job_state(self, queue_name: str, job_id: str) -> Optional[str]:
        job = await self.store.load(queue_name, job_id)
        return job.get("status") if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Optional[Any]:
        job = await self.store.load(queue_name, job_id)
        return job.get("result") if job else None

    async def add_dependencies(self, queue_name: str, job_dict: Dict[str, Any]) -> None:
        # Not implemented in MongoDB backend (yet)
        pass

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        # Not implemented in MongoDB backend (yet)
        pass

    async def pause_queue(self, queue_name: str) -> None:
        self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        return queue_name in self.paused

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        job = await self.store.load(queue_name, job_id)
        if job:
            job["progress"] = progress
            await self.store.save(queue_name, job_id, job)

    async def bulk_enqueue(self, queue_name: str, jobs: List[Dict[str, Any]]) -> None:
        async with self.lock:
            self.queues.setdefault(queue_name, []).extend(jobs)
            for job in jobs:
                await self.store.save(queue_name, job["id"], {**job, "status": State.WAITING})

    async def purge(self, queue_name: str, state: str, older_than: Optional[float] = None) -> None:
        jobs = await self.store.jobs_by_status(queue_name, state)
        now = time.time()
        for job in jobs:
            ts = job.get("completed_at", now)
            if older_than is None or ts < older_than:
                await self.store.delete(queue_name, job["id"])

    async def emit_event(self, event: str, data: Dict[str, Any]) -> None:
        await event_emitter.emit(event, data)

    async def create_lock(self, key: str, ttl: int) -> anyio.Lock:
        return anyio.Lock()
