import json
from typing import List, Optional

import redis.asyncio as redis

from asyncmq.stores.base import BaseJobStore


class RedisJobStore(BaseJobStore):
    def __init__(self, redis_url: str = "redis://localhost"):
        self.redis = redis.from_url(redis_url, decode_responses=True)

    def _key(self, queue_name: str, job_id: str) -> str:
        return f"jobs:{queue_name}:{job_id}"

    def _set_key(self, queue_name: str) -> str:
        return f"jobs:{queue_name}:ids"

    async def save(self, queue_name: str, job_id: str, data: dict):
        await self.redis.set(self._key(queue_name, job_id), json.dumps(data))
        await self.redis.sadd(self._set_key(queue_name), job_id)

    async def load(self, queue_name: str, job_id: str) -> Optional[dict]:
        raw = await self.redis.get(self._key(queue_name, job_id))
        return json.loads(raw) if raw else None

    async def delete(self, queue_name: str, job_id: str):
        await self.redis.delete(self._key(queue_name, job_id))
        await self.redis.srem(self._set_key(queue_name), job_id)

    async def all_jobs(self, queue_name: str) -> List[dict]:
        ids = await self.redis.smembers(self._set_key(queue_name))
        return [await self.load(queue_name, job_id) for job_id in ids if job_id]

    async def jobs_by_status(self, queue_name: str, status: str) -> List[dict]:
        all_jobs = await self.all_jobs(queue_name)
        return [job for job in all_jobs if job and job.get("status") == status]
