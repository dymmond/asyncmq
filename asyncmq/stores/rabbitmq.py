from typing import Any

import redis.asyncio as aioredis

from asyncmq.stores.base import BaseJobStore
from asyncmq.stores.redis_store import RedisJobStore


class RabbitMQJobStore(BaseJobStore):
    """
    Job metadata store implementation for the RabbitMQ backend.

    This class acts as an adapter, delegating the actual storage operations
    to an underlying `BaseJobStore` instance, typically a `RedisJobStore`.
    It provides methods for saving, loading, deleting, and querying job
    metadata based on queue names and job IDs.
    """

    def __init__(self, redis_url: str | None = None, backend: BaseJobStore | aioredis.Redis | None = None) -> None:
        """
        Initializes the RabbitMQJobStore instance.

        Args:
            redis_url: An optional Redis connection URL. This is used to
                initialize a `RedisJobStore` if no explicit `backend` is
                provided. Defaults to "redis://localhost" if not specified
                and no backend is given.
            backend: An optional pre-initialized backend. This can be either
                an instance of `BaseJobStore` (e.g., `RedisJobStore`) or a
                raw `redis.asyncio.Redis` client. If a `BaseJobStore` is
                provided, it's used directly. If a `redis.asyncio.Redis`
                client is provided, it overrides the internal Redis client
                of the default `RedisJobStore`.
        """
        # If an explicit BaseJobStore instance is provided, use it directly.
        if isinstance(backend, BaseJobStore):
            self._store: BaseJobStore = backend
        else:
            # Otherwise, initialize a RedisJobStore as the default underlying store.
            # Use the provided redis_url or default to "redis://localhost".
            self._store = RedisJobStore(redis_url or "redis://localhost")
            # If a raw Redis client is provided, override the RedisJobStore's
            # internal client with the provided one.
            if isinstance(backend, aioredis.Redis):
                self._store.redis = backend

    async def save(self, queue_name: str, job_id: str, data: dict[str, Any]) -> None:
        """
        Saves or updates the metadata for a specific job.

        This method delegates the actual saving operation to the underlying
        job store.

        Args:
            queue_name: The name of the queue to which the job belongs.
            job_id: The unique identifier of the job.
            data: A dictionary containing the job's metadata to be saved.
        """
        await self._store.save(queue_name, job_id, data)

    async def load(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        """
        Loads the metadata for a specific job by its ID.

        This method delegates the actual loading operation to the underlying
        job store.

        Args:
            queue_name: The name of the queue to which the job belongs.
            job_id: The unique identifier of the job to load.

        Returns:
            A dictionary containing the job's metadata if found, otherwise None.
        """
        return await self._store.load(queue_name, job_id)

    async def delete(self, queue_name: str, job_id: str) -> None:
        """
        Deletes the metadata for a specific job by its ID.

        This method delegates the actual deletion operation to the underlying
        job store.

        Args:
            queue_name: The name of the queue from which to delete the job.
            job_id: The unique identifier of the job to delete.
        """
        await self._store.delete(queue_name, job_id)

    async def all_jobs(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Retrieves all jobs associated with a specific queue.

        This method delegates the retrieval operation to the underlying job store.

        Args:
            queue_name: The name of the queue for which to retrieve all jobs.

        Returns:
            A list of dictionaries, where each dictionary represents the
            metadata of a job in the specified queue.
        """
        return await self._store.all_jobs(queue_name)

    async def jobs_by_status(self, queue_name: str, status: str) -> list[dict[str, Any]]:
        """
        Retrieves jobs from a specific queue, filtered by their status.

        This method delegates the filtered retrieval operation to the
        underlying job store.

        Args:
            queue_name: The name of the queue to retrieve jobs from.
            status: The status string to filter jobs by (e.g., 'waiting',
                'completed', 'failed').

        Returns:
            A list of dictionaries, where each dictionary represents the
            metadata of a job matching the specified status in the queue.
        """
        return await self._store.jobs_by_status(queue_name, status)
