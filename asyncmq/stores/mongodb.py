from typing import Any

try:
    import motor  # noqa
except ImportError:
    raise ImportError("Please install motor: `pip install motor`") from None

import motor.motor_asyncio

from asyncmq.stores.base import BaseJobStore


class MongoDBStore(BaseJobStore):
    """
    MongoDB-based job store for AsyncMQ.

    Provides persistent storage for AsyncMQ job payloads using a MongoDB
    collection. It handles saving, loading, deleting, and querying jobs
    based on queue name and status. This store requires the `connect` async
    method to be called before use to ensure indexes are initialized.
    """

    def __init__(self, mongo_url: str = "mongodb://localhost", database: str = "asyncmq") -> None:
        """
        Initializes the MongoDB job store client and collection references.

        Sets up the connection to the specified MongoDB instance and database,
        and selects or creates the 'jobs' collection. Note that database indexes
        are initialized asynchronously in the `connect` method, not here.

        Args:
            mongo_url: The MongoDB connection URL. Defaults to "mongodb://localhost".
            database: The name of the MongoDB database to use. Defaults to "asyncmq".
        """
        # Create an asynchronous MongoDB client instance.
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)
        # Access the specified database.
        self.db = self.client[database]
        # Access the 'jobs' collection within the database.
        self.collection = self.db["jobs"]

    async def connect(self) -> None:
        """
        Initializes required indexes on the MongoDB collection.

        This asynchronous method must be called once after creating the store
        instance and before performing any save/load/delete operations to
        ensure database indexes are in place for performance and data integrity.
        """
        # Ensure a unique compound index on queue_name and job_id for efficient
        # save/load/delete operations and to prevent duplicates. background=False
        # means index creation will block until complete.
        await self.collection.create_index([("queue_name", 1), ("job_id", 1)], unique=True, background=False)
        # Ensure an index on the 'status' field for efficient status-based queries.
        # background=False means index creation will block until complete.
        await self.collection.create_index("status", background=False)

    async def save(self, queue_name: str, job_id: str, data: dict[str, Any]) -> None:
        """
        Saves or updates a job document in the MongoDB collection.

        If a job with the given queue name and job ID exists, it is updated.
        Otherwise, a new document is created.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            data: A dictionary containing the job payload and state information.
        """
        # Create a mutable copy of the input data dictionary.
        data = dict(data)
        # Add or update 'queue_name' and 'job_id' fields in the document.
        data["queue_name"] = queue_name
        data["job_id"] = job_id
        # Use update_one with upsert=True to insert if the document doesn't exist
        # or update if it does, based on the queue_name and job_id filter.
        await self.collection.update_one(
            {"queue_name": queue_name, "job_id": job_id},
            {"$set": data},
            upsert=True,
        )

    async def load(self, queue_name: str, job_id: str) -> Any:
        """
        Loads a job document from the MongoDB collection by its ID and queue name.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            A dictionary representing the job document if found, otherwise None.
        """
        # Find a single document matching the queue name and job ID.
        return await self.collection.find_one({"queue_name": queue_name, "job_id": job_id})

    async def delete(self, queue_name: str, job_id: str) -> None:
        """
        Deletes a job document from the MongoDB collection by its ID and queue name.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to delete.
        """
        # Delete a single document matching the queue name and job ID.
        await self.collection.delete_one({"queue_name": queue_name, "job_id": job_id})

    async def all_jobs(self, queue_name: str) -> Any:
        """
        Retrieves all job documents for a specific queue.

        Args:
            queue_name: The name of the queue to retrieve jobs from.

        Returns:
            A list of dictionaries, each representing a job document.
        """
        # Find all documents matching the queue name.
        cursor = self.collection.find({"queue_name": queue_name})
        # Convert the asynchronous cursor results to a list. length=None fetches all.
        return await cursor.to_list(length=None)

    async def jobs_by_status(self, queue_name: str, status: str) -> Any:
        """
        Retrieves job documents for a specific queue and status.

        Args:
            queue_name: The name of the queue to retrieve jobs from.
            status: The status of the jobs to filter by.

        Returns:
            A list of dictionaries, each representing a job document with the
            specified status.
        """
        # Find documents matching both the queue name and status.
        cursor = self.collection.find({"queue_name": queue_name, "status": status})
        # Convert the asynchronous cursor results to a list. length=None fetches all.
        return await cursor.to_list(length=None)

    async def filter(self, queue: str, state: str) -> list[dict[str, Any]]:
        await self.connect()
        cursor = self.collection.find({"queue_name": queue, "status": state})
        jobs = []
        async for doc in cursor:
            doc.pop("_id", None)  # Optional: remove internal MongoDB ID
            jobs.append(doc)
        return jobs
