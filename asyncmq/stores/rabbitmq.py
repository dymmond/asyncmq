from typing import Any, Dict, List, Optional

from asyncmq.backends.base import BaseBackend
from asyncmq.stores.base import BaseJobStore
from asyncmq.stores.redis_store import RedisJobStore


class RabbitMQJobStore(BaseJobStore):
    """
    Job metadata store for RabbitMQ backend.
    Internally delegates to RedisJobStore for persistence.
    """

    def __init__(
        self,
        redis_url: str | None = None,
        backend: BaseBackend | None = None
    ):
        assert redis_url is not None or backend is not None, "a redis_url must be provided or a different backend at your choice"

        # Delegate storage to Redis/another
        self._store = RedisJobStore(redis_url) if redis_url is not None else backend

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
        return await self._store.get(queue_name, job_id)

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
        return await self._store.list_jobs(queue_name)

    async def jobs_by_status(
        self,
        queue_name: str,
        status: str
    ) -> List[Dict[str, Any]]:
        """
        Retrieve jobs filtered by status.
        """
        return await self._store.get_by_status(queue_name, status)
