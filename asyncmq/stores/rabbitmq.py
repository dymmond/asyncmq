from typing import Any, Dict, List, Optional, Union

import redis.asyncio as aioredis

from asyncmq.stores.base import BaseJobStore
from asyncmq.stores.redis_store import RedisJobStore


class RabbitMQJobStore(BaseJobStore):
    """
    Job metadata store for RabbitMQ backend.
    Delegates storage to a RedisJobStore or any BaseJobStore.
    """

    def __init__(
        self,
        redis_url: Optional[str] = None,
        backend: Optional[Union[BaseJobStore, aioredis.Redis]] = None
    ) -> None:
        # If provided, a BaseJobStore instance is used directly
        if isinstance(backend, BaseJobStore):
            self._store: BaseJobStore = backend
        else:
            # Otherwise, initialize a RedisJobStore
            self._store = RedisJobStore(redis_url or "redis://localhost")
            # If a raw Redis client is provided, override the internal client
            if isinstance(backend, aioredis.Redis):
                self._store.redis = backend

    async def save(
        self,
        queue_name: str,
        job_id: str,
        data: Dict[str, Any]
    ) -> None:
        """
        Save or update job data.
        """
        await self._store.save(queue_name, job_id, data)

    async def load(
        self,
        queue_name: str,
        job_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Load job data by ID.
        """
        return await self._store.load(queue_name, job_id)

    async def delete(
        self,
        queue_name: str,
        job_id: str
    ) -> None:
        """
        Delete job data by ID.
        """
        await self._store.delete(queue_name, job_id)

    async def all_jobs(
        self,
        queue_name: str
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs for a queue.
        """
        return await self._store.all_jobs(queue_name)

    async def jobs_by_status(
        self,
        queue_name: str,
        status: str
    ) -> List[Dict[str, Any]]:
        """
        Retrieve jobs filtered by status.
        """
        return await self._store.jobs_by_status(queue_name, status)
