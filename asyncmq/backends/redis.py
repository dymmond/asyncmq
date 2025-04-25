import json
import time
from typing import Optional

import redis.asyncio as redis

from asyncmq.backends.base import BaseBackend
from asyncmq.stores.redis_store import RedisJobStore


class RedisBackend(BaseBackend):
    def __init__(self, redis_url: str = "redis://localhost"):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.job_store = RedisJobStore(redis_url)

    def _queue_key(self, name: str) -> str:
        return f"queue:{name}"

    def _dlq_key(self, name: str) -> str:
        return f"queue:{name}:dlq"

    def _delayed_key(self, name: str) -> str:
        return f"queue:{name}:delayed"

    async def enqueue(self, queue_name: str, payload: dict):
        await self.redis.lpush(self._queue_key(queue_name), json.dumps(payload))
        await self.job_store.save(queue_name, payload["id"], payload)

    async def dequeue(self, queue_name: str) -> Optional[dict]:
        raw = await self.redis.rpop(self._queue_key(queue_name))
        if raw:
            payload = json.loads(raw)
            return payload
        return None

    async def move_to_dlq(self, queue_name: str, payload: dict):
        await self.redis.lpush(self._dlq_key(queue_name), json.dumps(payload))
        payload["status"] = "failed"
        await self.job_store.save(queue_name, payload["id"], payload)

    async def ack(self, queue_name: str, job_id: str):
        # Optionally delete from Redis queue; here we leave it for historical querying
        pass

    async def enqueue_delayed(self, queue_name: str, payload: dict, run_at: float):
        await self.redis.zadd(self._delayed_key(queue_name), {json.dumps(payload): run_at})
        payload["status"] = "delayed"
        await self.job_store.save(queue_name, payload["id"], payload)

    async def get_due_delayed(self, queue_name: str) -> list[dict]:
        now = time.time()
        raw_jobs = await self.redis.zrangebyscore(self._delayed_key(queue_name), 0, now)
        jobs = [json.loads(raw) for raw in raw_jobs]
        return jobs

    async def remove_delayed(self, queue_name: str, job_id: str):
        all_jobs = await self.redis.zrange(self._delayed_key(queue_name), 0, -1)
        for raw in all_jobs:
            job = json.loads(raw)
            if job["id"] == job_id:
                await self.redis.zrem(self._delayed_key(queue_name), raw)
                return

    async def update_job_state(self, queue_name: str, job_id: str, state: str):
        job = await self.job_store.load(queue_name, job_id)
        if job:
            job["status"] = state
            await self.job_store.save(queue_name, job_id, job)

    async def save_job_result(self, queue_name: str, job_id: str, result: any):
        job = await self.job_store.load(queue_name, job_id)
        if job:
            job["result"] = result
            await self.job_store.save(queue_name, job_id, job)

    async def get_job_state(self, queue_name: str, job_id: str) -> Optional[str]:
        job = await self.job_store.load(queue_name, job_id)
        return job.get("status") if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Optional[any]:
        job = await self.job_store.load(queue_name, job_id)
        return job.get("result") if job else None
