import asyncio
import time
from typing import Any, Optional

from asyncmq.backends.base import BaseBackend


class InMemoryBackend(BaseBackend):
    def __init__(self):
        self.queues = {} # {queue_name: list of jobs}
        self.dlqs = {}    # {queue_name: list}
        self.delayed = {} # {queue_name: list of (run_at, job_dict)}
        self.job_states = {}  # {(queue_name, job_id): str}
        self.job_results = {}  # {(queue_name, job_id): Any}
        self.lock = asyncio.Lock()

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
                return queue.pop(0)  # Pop highest priority job
            return None

    async def move_to_dlq(self, queue_name: str, payload: dict):
        async with self.lock:
            dlq = self.dlqs.setdefault(queue_name, [])
            dlq.append(payload)
            self.job_states[(queue_name, payload["id"])] = "failed"

    async def ack(self, queue_name: str, job_id: str):
        # No-op for in-memory, could be used for cleanup in other backends
        ...

    async def enqueue_delayed(self, queue_name: str, payload: dict, run_at: float):
        async with self.lock:
            delayed_list = self.delayed.setdefault(queue_name, [])
            delayed_list.append((run_at, payload))
            self.job_states[(queue_name, payload["id"])] = "delayed"

    async def get_due_delayed(self, queue_name: str) -> list[dict]:
        async with self.lock:
            now = time.time()
            due_jobs = []
            delayed_list = self.delayed.get(queue_name, [])
            still_delayed = []
            for run_at, payload in delayed_list:
                if run_at <= now:
                    due_jobs.append(payload)
                else:
                    still_delayed.append((run_at, payload))
            self.delayed[queue_name] = still_delayed
            return due_jobs

    async def remove_delayed(self, queue_name: str, job_id: str):
        # Already removed in get_due_delayed, so this is a no-op here
        ...

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
