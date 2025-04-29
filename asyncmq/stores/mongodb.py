try:
    import motor
except ImportError:
    raise ImportError("Please install motor: `pip install motor`") from None

from typing import Any

import motor.motor_asyncio

from asyncmq.stores.base import BaseJobStore


class MongoDBStore(BaseJobStore):
    """
    MongoDB-based job store for AsyncMQ.

    Stores full job payloads in a MongoDB collection. Supports saving, loading,
    deleting, and filtering jobs by status.
    """
    def __init__(self, mongo_url: str = "mongodb://localhost", database: str = "asyncmq") -> None:
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)
        self.db = self.client[database]
        self.collection = self.db["jobs"]

        # Ensure indexes for efficiency and uniqueness
        self.collection.create_index([("queue_name", 1), ("job_id", 1)], unique=True)
        self.collection.create_index("status")

    async def save(self, queue_name: str, job_id: str, data: dict[str, Any]) -> None:
        data = dict(data)
        data["queue_name"] = queue_name
        data["job_id"] = job_id
        await self.collection.update_one(
            {"queue_name": queue_name, "job_id": job_id},
            {"$set": data},
            upsert=True,
        )

    async def load(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        doc = await self.collection.find_one({"queue_name": queue_name, "job_id": job_id})
        return doc

    async def delete(self, queue_name: str, job_id: str) -> None:
        await self.collection.delete_one({"queue_name": queue_name, "job_id": job_id})

    async def all_jobs(self, queue_name: str) -> list[dict[str, Any]]:
        cursor = self.collection.find({"queue_name": queue_name})
        return await cursor.to_list(length=None)

    async def jobs_by_status(self, queue_name: str, status: str) -> list[dict[str, Any]]:
        cursor = self.collection.find({"queue_name": queue_name, "status": status})
        return await cursor.to_list(length=None)
