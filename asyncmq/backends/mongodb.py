import time
from typing import Any, cast

import anyio

# Conditional import for motor, handled with a try/except block.
# Motor is an asynchronous MongoDB driver.
try:
    import motor  # noqa: F401 # Import motor, ignore unused warning if not directly called in this file
    from pymongo import ReturnDocument
except ImportError:
    # If motor is not installed, raise a specific ImportError with a helpful message.
    # The 'from None' prevents chaining the original ImportError exception.
    raise ImportError("Please install motor: `pip install motor`") from None

# Import necessary components from asyncmq.
from asyncmq.backends.base import (
    BaseBackend,
    DelayedInfo,
    RepeatableInfo,
    WorkerInfo,
)
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.core.inspection import has_pending_dependencies, matches_job_type, normalize_job_type
from asyncmq.core.repeatables import normalize_repeatable_job_def, repeatable_identity
from asyncmq.schedulers import compute_next_run
from asyncmq.stores.mongodb import MongoDBStore


class MongoDBBackend(BaseBackend):
    """
    MongoDB backend implementation for AsyncMQ.

    This backend manages job queues, delayed jobs, repeatable jobs, and job
    states using an in-memory representation synchronized by an `anyio.Lock`,
    along with a `MongoDBStore` for persistent storage of job data. It requires
    calling the `connect` async method after initialization to establish the
    database connection.
    """

    def __init__(self, mongo_url: str = "mongodb://localhost", database: str = "asyncmq") -> None:
        """
        Initializes the MongoDB backend and its underlying store and in-memory state.

        Creates an instance of the `MongoDBStore` for persistent storage and
        initializes in-memory data structures (dictionaries and sets) to
        represent queues, delayed jobs, paused queues, repeatable jobs,
        cancelled jobs, and heartbeats. An `anyio.Lock` is initialized for
        synchronizing access to these in-memory structures.

        Args:
            mongo_url: The MongoDB connection URL string. Defaults to "mongodb://localhost".
            database: The name of the MongoDB database to use for storing data.
                      Defaults to "asyncmq".
        """
        # Initialize the MongoDB store for persistent job data storage.
        self.store: MongoDBStore = MongoDBStore(mongo_url, database)
        # In-memory representation of waiting queues: maps queue names to lists of job payloads.
        self.queues: dict[str, list[dict[str, Any]]] = {}
        # In-memory representation of delayed jobs: maps queue names to lists of (run_at, job) tuples.
        self.delayed: dict[str, list[tuple[float, dict[str, Any]]]] = {}
        # In-memory set of queue names that are currently paused.
        self.paused: set[str] = set()
        # Repeatable definitions are stored durably in MongoDB using status="repeatable".
        self.repeatables: dict[str, dict[str, dict[str, Any]]] = {}
        # Process-local cache of durably cancelled jobs: queue_name -> set(job_id).
        self.cancelled: dict[str, set[str]] = {}
        # An anyio Lock used to synchronize access to the in-memory data structures.
        self.lock: anyio.Lock = anyio.Lock()
        # In-memory heartbeats: maps (queue_name, job_id) tuples to their last heartbeat timestamp.
        self.heartbeats: dict[tuple[str, str], float] = {}
        self._workers = self.store.db["worker_heartbeats"]
        self._queue_controls = self.store.db["queue_controls"]
        self._cancelled_jobs = self.store.db["cancelled_jobs"]
        self._locks: dict[str, anyio.Lock] = {}

    async def connect(self) -> None:
        """
        Asynchronously connects the backend to the MongoDB database and initializes indexes.

        This method must be called after creating the backend instance and before
        performing any database operations to establish the connection and ensure
        that the necessary indexes for efficient querying are created in the
        MongoDB store.
        """
        # Call the connect method of the underlying MongoDBStore to establish the connection.
        await self.store.connect()
        await self._queue_controls.create_index("queue_name", unique=True, background=False)
        await self.store.collection.create_index(
            [("queue_name", 1), ("status", 1), ("priority", 1), ("created_at", 1), ("job_id", 1)],
            background=False,
        )
        await self.store.collection.create_index(
            [("queue_name", 1), ("status", 1), ("delay_until", 1)],
            background=False,
        )
        await self._cancelled_jobs.create_index([("queue_name", 1), ("job_id", 1)], unique=True, background=False)

    async def _mark_job_cancelled(self, queue_name: str, job_id: str) -> None:
        now = time.time()
        await self._cancelled_jobs.update_one(
            {"queue_name": queue_name, "job_id": job_id},
            {"$set": {"queue_name": queue_name, "job_id": job_id, "cancelled_at": now}},
            upsert=True,
        )

    async def _clear_job_cancelled(self, queue_name: str, job_id: str) -> None:
        await self._cancelled_jobs.delete_one({"queue_name": queue_name, "job_id": job_id})

    async def _is_job_durably_cancelled(self, queue_name: str, job_id: str) -> bool:
        marker = await self._cancelled_jobs.find_one({"queue_name": queue_name, "job_id": job_id}, {"_id": 1})
        return marker is not None

    async def _persist_cancelled_job_state(self, queue_name: str, job_id: str) -> None:
        now = time.time()
        await self.store.collection.update_one(
            {"queue_name": queue_name, "job_id": job_id},
            {"$set": {"status": "cancelled", "cancelled_at": now, "updated_at": now}},
        )

    async def pop_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Atomically fetch and remove all delayed jobs whose run_at ≤ now.
        Delegates to get_due_delayed(), which does the in-memory removal
        under self.lock.
        """
        return await self.get_due_delayed(queue_name)

    async def promote_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        promoted: list[dict[str, Any]] = []
        await self.connect()
        now = time.time()
        cursor = self.store.collection.find(
            {"queue_name": queue_name, "status": State.DELAYED, "delay_until": {"$lte": now}}
        ).sort([("delay_until", 1), ("priority", 1), ("created_at", 1), ("job_id", 1)])
        due_docs = await cursor.to_list(length=None)

        async with self.lock:
            queue = self.queues.setdefault(queue_name, [])
            for payload in due_docs:
                job_id = str(payload.get("id") or payload.get("job_id"))
                updated = await self.store.collection.find_one_and_update(
                    {
                        "queue_name": queue_name,
                        "job_id": job_id,
                        "status": State.DELAYED,
                        "delay_until": {"$lte": now},
                    },
                    {"$set": {"status": State.WAITING, "delay_until": None}},
                    return_document=ReturnDocument.AFTER,
                )
                if updated is None:
                    continue

                updated.pop("_id", None)
                waiting_payload = dict(updated)
                queue.append(waiting_payload)
                promoted.append(waiting_payload)

            promoted_ids = {str(item.get("id") or item.get("job_id")) for item in promoted}
            self.delayed[queue_name] = [
                (run_at, job)
                for run_at, job in self.delayed.get(queue_name, [])
                if str(job.get("id") or job.get("job_id")) not in promoted_ids
            ]
            queue.sort(key=lambda job: job.get("priority", 5))
        return promoted

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> str:
        """
        Asynchronously adds a job to the specified queue.

        Adds the job payload to the in-memory queue list for the given queue name
        and saves the job to the MongoDB store with its status set to `State.WAITING`.
        Access to the in-memory queue is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to add the job to.
            payload: A dictionary containing the job data, which must include a
                     unique identifier (usually 'id').
        """
        # Acquire the lock to ensure exclusive access to in-memory queues.
        async with self.lock:
            job_id = str(payload["id"])
            waiting_payload = {**payload, "status": State.WAITING, "created_at": payload.get("created_at", time.time())}
            queue = self.queues.setdefault(queue_name, [])
            self.queues[queue_name] = [job for job in queue if str(job.get("id")) != job_id]
            if not has_pending_dependencies(waiting_payload):
                self.queues[queue_name].append(waiting_payload)
                self.queues[queue_name].sort(key=lambda job: job.get("priority", 5))
            await self.store.save(queue_name, job_id, waiting_payload)
            return job_id

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously attempts to dequeue the next available job from the specified queue.

        This method checks the in-memory queue list for the given queue name. If
        jobs are available, it removes and returns the first job, updating its
        status to `State.ACTIVE` in the MongoDB store. Returns None if the queue
        is empty. Access to the in-memory queue is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to dequeue from.

        Returns:
            The job dictionary if a job was successfully dequeued, otherwise None.
        """
        await self.connect()
        active_since = time.time()
        claimed = await self.store.collection.find_one_and_update(
            {
                "queue_name": queue_name,
                "status": State.WAITING,
                "$or": [
                    {"depends_on": {"$exists": False}},
                    {"depends_on": None},
                    {"depends_on": []},
                ],
            },
            {"$set": {"status": State.ACTIVE, "active_since": active_since, "updated_at": active_since}},
            sort=[("priority", 1), ("created_at", 1), ("job_id", 1)],
            return_document=ReturnDocument.AFTER,
        )
        while claimed is not None:
            claimed.pop("_id", None)
            job = dict(claimed)
            job_id = str(job.get("id") or job.get("job_id"))
            async with self.lock:
                self.queues[queue_name] = [
                    queued for queued in self.queues.get(queue_name, []) if str(queued.get("id")) != job_id
                ]
            if await self.is_job_cancelled(queue_name, job_id):
                await self.cancel_active_job(queue_name, job)
                claimed = await self.store.collection.find_one_and_update(
                    {
                        "queue_name": queue_name,
                        "status": State.WAITING,
                        "$or": [
                            {"depends_on": {"$exists": False}},
                            {"depends_on": None},
                            {"depends_on": []},
                        ],
                    },
                    {"$set": {"status": State.ACTIVE, "active_since": active_since, "updated_at": active_since}},
                    sort=[("priority", 1), ("created_at", 1), ("job_id", 1)],
                    return_document=ReturnDocument.AFTER,
                )
                continue
            return job
        return None

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously marks a job as failed and effectively moves it to the
        Dead-Letter Queue (DLQ) by updating its status in the MongoDB store.

        This implementation updates the job's status to `State.FAILED` in the
        persistent MongoDB store. The job is not moved to a separate in-memory
        DLQ structure in this backend. Access to the in-memory state (though
        not directly modified here) is protected by the lock for consistency.

        Args:
            queue_name: The name of the queue the job originally belonged to.
            payload: A dictionary containing the job data, which must include an 'id'.
        """
        async with self.lock:
            job_id = payload.get("id")
            if isinstance(job_id, str):
                self.queues[queue_name] = [job for job in self.queues.get(queue_name, []) if job.get("id") != job_id]
                self.delayed[queue_name] = [
                    (run_at, job) for run_at, job in self.delayed.get(queue_name, []) if job.get("id") != job_id
                ]
                self.heartbeats.pop((queue_name, job_id), None)

            target_status = payload.get("status")
            if target_status not in {State.FAILED, State.EXPIRED}:
                target_status = State.FAILED
            failed_payload = {**payload, "status": target_status}
            await self.store.save(queue_name, failed_payload["id"], failed_payload)

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        In this MongoDB backend implementation, explicit acknowledgment via this
        method is not strictly necessary for managing job state transitions like
        COMPLETED or FAILED, as these updates are handled by the `update_job_state`
        method which interacts with the persistent store. Therefore, this method
        is a no-operation (no-op).

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job being acknowledged.
        """
        # Nothing required here; completion/failure state is updated elsewhere
        # by calls to update_job_state.
        pass

    def _remove_job_memberships_locked(self, queue_name: str, job_id: str) -> None:
        self.queues[queue_name] = [job for job in self.queues.get(queue_name, []) if str(job.get("id")) != job_id]
        self.delayed[queue_name] = [
            (run_at, job) for run_at, job in self.delayed.get(queue_name, []) if str(job.get("id")) != job_id
        ]
        self.heartbeats.pop((queue_name, job_id), None)

    async def _replace_lifecycle_payload_locked(self, queue_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        job_id = str(payload["id"])
        stored = dict(payload)
        stored.pop("_id", None)
        stored["queue_name"] = queue_name
        stored["job_id"] = job_id
        await self.store.collection.replace_one(
            {"queue_name": queue_name, "job_id": job_id, "status": {"$ne": "cancelled"}},
            stored,
        )
        return stored

    async def complete_active_job(self, queue_name: str, payload: dict[str, Any], result: Any) -> None:
        job_id = str(payload["id"])
        if await self.is_job_cancelled(queue_name, job_id):
            await self.cancel_active_job(queue_name, payload)
            return
        stored = {**payload, "status": State.COMPLETED, "result": result, "delay_until": None}
        async with self.lock:
            self._remove_job_memberships_locked(queue_name, job_id)
            await self._replace_lifecycle_payload_locked(queue_name, stored)

    async def retry_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        await self._delay_active_job(queue_name, payload, run_at)

    async def defer_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        await self._delay_active_job(queue_name, payload, run_at)

    async def _delay_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        job_id = str(payload["id"])
        if await self.is_job_cancelled(queue_name, job_id):
            await self.cancel_active_job(queue_name, payload)
            return
        stored = {**payload, "status": State.DELAYED, "delay_until": run_at}
        async with self.lock:
            self._remove_job_memberships_locked(queue_name, job_id)
            persisted = await self._replace_lifecycle_payload_locked(queue_name, stored)
            self.delayed.setdefault(queue_name, []).append((run_at, persisted))

    async def fail_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        target_status = payload.get("status")
        if target_status not in {State.FAILED, State.EXPIRED}:
            target_status = State.FAILED
        job_id = str(payload["id"])
        if await self.is_job_cancelled(queue_name, job_id):
            await self.cancel_active_job(queue_name, payload)
            return
        stored = {**payload, "status": target_status, "delay_until": None}
        async with self.lock:
            self._remove_job_memberships_locked(queue_name, job_id)
            await self._replace_lifecycle_payload_locked(queue_name, stored)

    async def expire_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        job_id = str(payload["id"])
        if await self.is_job_cancelled(queue_name, job_id):
            await self.cancel_active_job(queue_name, payload)
            return
        stored = {**payload, "status": State.EXPIRED, "delay_until": None}
        async with self.lock:
            self._remove_job_memberships_locked(queue_name, job_id)
            await self._replace_lifecycle_payload_locked(queue_name, stored)

    async def cancel_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        job_id = str(payload["id"])
        await self.connect()
        await self._mark_job_cancelled(queue_name, job_id)
        async with self.lock:
            self._remove_job_memberships_locked(queue_name, job_id)
            self.cancelled.setdefault(queue_name, set()).add(job_id)
            await self._persist_cancelled_job_state(queue_name, job_id)

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Asynchronously adds a job to the in-memory delayed queue to be processed
        at a later time.

        Adds the job payload and its scheduled run time (`run_at`) to the
        in-memory delayed queue list for the given queue name. It also saves
        the job to the MongoDB store with its status set to `State.EXPIRED`.
        Access to the in-memory delayed queue is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job will eventually be added to.
            payload: A dictionary containing the job data, which must include an 'id'.
            run_at: A timestamp (float, typically seconds since the epoch)
                    indicating when the job should become eligible for processing.
        """
        # Acquire the lock to ensure exclusive access to in-memory delayed queues.
        async with self.lock:
            # Add the job and its scheduled run time to the in-memory delayed list.
            # setdefault ensures the list exists even if this is the first delayed job for this queue.
            delayed_payload = {
                **payload,
                "status": State.DELAYED,
                "delay_until": run_at,
                "created_at": payload.get("created_at", time.time()),
            }
            self.delayed.setdefault(queue_name, []).append((run_at, delayed_payload))
            await self.store.save(queue_name, payload["id"], delayed_payload)

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves delayed jobs from the specified queue that are
        now due for processing.

        Checks the in-memory delayed queue list for jobs whose scheduled run time
        (`run_at`) is less than or equal to the current time. It removes these
        due jobs from the in-memory list and returns them. Access to the in-memory
        delayed queue is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of job dictionaries that are ready to be processed.
        """
        await self.connect()
        now = time.time()
        cursor = self.store.collection.find(
            {"queue_name": queue_name, "status": State.DELAYED, "delay_until": {"$lte": now}}
        ).sort([("delay_until", 1), ("priority", 1), ("created_at", 1), ("job_id", 1)])
        due_docs = await cursor.to_list(length=None)
        due_ids = {str(job.get("id") or job.get("job_id")) for job in due_docs}
        async with self.lock:
            self.delayed[queue_name] = [
                (run_at, job)
                for run_at, job in self.delayed.get(queue_name, [])
                if str(job.get("id") or job.get("job_id")) not in due_ids
            ]

        due: list[dict[str, Any]] = []
        for job in due_docs:
            job.pop("_id", None)
            due.append(job)
        return due

    async def remove_delayed(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously removes a job from the in-memory delayed queue by its ID.

        This method iterates through the in-memory delayed queue list for the
        given queue name and removes the tuple containing the job with the
        matching ID. Access to the in-memory delayed queue is synchronized
        using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove from the delayed queue.
        """
        await self.connect()
        result = await self.store.collection.delete_one(
            {"queue_name": queue_name, "job_id": job_id, "status": State.DELAYED},
        )
        async with self.lock:
            # Filter the in-memory delayed list, keeping only jobs whose ID does
            # not match the one to be removed. Use .get() with default [] for safety.
            before = len(self.delayed.get(queue_name, []))
            self.delayed[queue_name] = [
                (ts, job) for ts, job in self.delayed.get(queue_name, []) if job.get("id") != job_id
            ]
            return result.deleted_count > 0 or len(self.delayed.get(queue_name, [])) < before

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the processing state of a job in the MongoDB store.

        Loads the job data from the persistent store using the job ID, updates its
        'status' field with the new state string, and saves the modified job data
        back to the store. This method primarily interacts with the persistent
        store, but the lock is acquired for consistency with other operations.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to update.
            state: A string representing the new state of the job
                   (e.g., 'active', 'completed', 'failed').
        """
        # Acquire the lock for consistency, although primarily interacting with the store.
        async with self.lock:
            # Load the job data from the MongoDB store.
            job = await self.store.load(queue_name, job_id)
            # If the job data was successfully loaded.
            if job:
                job["status"] = state  # Update the status field with the new state.
                if state == State.ACTIVE:
                    job.setdefault("active_since", time.time())
                await self.store.save(queue_name, job_id, job)  # Save the updated job data.

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a completed job in the MongoDB store.

        Loads the job data from the persistent store using the job ID, adds or
        updates the 'result' field with the execution result, and saves the modified
        job data back to the store. The lock is acquired for consistency.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job whose result is to be saved.
            result: The result data of the job. This should be a type that is
                    JSON serializable by the underlying job store.
        """
        # Acquire the lock for consistency, although primarily interacting with the store.
        async with self.lock:
            # Load the job data from the MongoDB store.
            job = await self.store.load(queue_name, job_id)
            # If the job data was successfully loaded.
            if job:
                job["result"] = result  # Add or update the result field.
                await self.store.save(queue_name, job_id, job)  # Save the updated job data.

    async def save_job_payload(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Persist an updated canonical payload in MongoDB and in-memory mirrors.

        This method is used by queue administration APIs that need to edit job
        metadata without moving the job between runtime buckets.

        Args:
            queue_name: The queue that owns the job.
            payload: The full canonical payload to persist.
        """
        job_id = str(payload["id"])
        stored = dict(payload)
        async with self.lock:
            document = dict(stored)
            document.pop("_id", None)
            document["queue_name"] = queue_name
            document["job_id"] = job_id
            await self.store.collection.replace_one(
                {"queue_name": queue_name, "job_id": job_id},
                document,
                upsert=True,
            )

            waiting = self.queues.get(queue_name, [])
            for index, job in enumerate(waiting):
                if job.get("id") == job_id:
                    waiting[index] = dict(stored)

            delayed = self.delayed.get(queue_name, [])
            for index, (run_at, job) in enumerate(delayed):
                if job.get("id") == job_id:
                    delayed[index] = (run_at, dict(stored))

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Asynchronously retrieves the current status string of a specific job
        from the MongoDB store.

        Loads the job data from the persistent store using the job ID and returns
        the value of the 'status' field if the job is found. Returns None if the
        job is not found or does not have a 'status' field. The lock is acquired
        for consistency.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job whose state is requested.

        Returns:
            The job's status string if the job is found and has a status,
            otherwise None.
        """
        # Acquire the lock for consistency, although primarily interacting with the store.
        async with self.lock:
            # Load the job data from the MongoDB store.
            job = await self.store.load(queue_name, job_id)
            # Return the 'status' field if the job is found, otherwise None.
            return cast(str, job.get("status")) if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Asynchronously retrieves the execution result of a specific job from
        the MongoDB store.

        Loads the job data from the persistent store using the job ID and returns
        the value of the 'result' field if the job is found and has a result.
        Returns None if the job is not found or does not have a 'result' field.
        The lock is acquired for consistency.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job whose result is requested.

        Returns:
            The job's result data if the job is found and has a result,
            otherwise None.
        """
        # Acquire the lock for consistency, although primarily interacting with the store.
        async with self.lock:
            # Load the job data from the MongoDB store.
            job = await self.store.load(queue_name, job_id)
            # Return the 'result' field if the job is found, otherwise None.
            return job.get("result") if job else None

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Asynchronously registers dependencies for a single job in the MongoDB store.

        This method loads the job from the persistent store, adds the parent job
        IDs specified in the 'depends_on' field of the `job_dict` to the job's
        existing 'depends_on' list (if any), and saves the updated job back to
        the store. The lock is acquired for consistency.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The dictionary representing the job, which must contain
                      an 'id' and is expected to contain a 'depends_on' list
                      of parent job IDs.
        """
        # Acquire the lock for consistency.
        async with self.lock:
            # Load the job data from the MongoDB store using the job ID from the input dict.
            job = await self.store.load(queue_name, job_dict["id"])
            # If the job data was successfully loaded.
            if job:
                # Get the list of parent job IDs this job depends on from the input dict.
                pending: list[str] = job_dict.get("depends_on", [])
                # Get the existing dependencies list from the loaded job data, defaulting to empty.
                existing_deps: list[str] = job.get("depends_on", [])

                # Iterate through the new pending dependencies.
                for parent in pending:
                    # If the parent is not already in the existing dependencies list.
                    if parent not in existing_deps:
                        # Add the parent to the existing dependencies list.
                        existing_deps.append(parent)

                # Update the 'depends_on' field in the job data with the combined list.
                job["depends_on"] = existing_deps
                self.queues[queue_name] = [
                    queued for queued in self.queues.get(queue_name, []) if str(queued.get("id")) != str(job["id"])
                ]
                # Save the updated job data back to the MongoDB store.
                await self.store.save(queue_name, job["id"], job)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and resolves
        dependencies for any child jobs waiting on it in the MongoDB store.

        This method finds jobs in the persistent store that depend on the `parent_id`.
        For each dependent child job, it removes the `parent_id` from the child's
        'depends_on' list. If a child job's 'depends_on' list becomes empty, it means
        all its dependencies are met, and the child job's status is updated to
        `State.WAITING` and it is added to the in-memory queue for immediate processing.
        The lock is acquired for consistency.

        Args:
            queue_name: The name of the queue containing the dependent jobs.
            parent_id: The unique identifier of the parent job that has completed.
        """
        async with self.lock:
            all_jobs = await self.store.all_jobs(queue_name)
            for job in all_jobs:
                depends_on: list[str] = list(job.get("depends_on", []))
                if parent_id not in depends_on:
                    continue

                depends_on.remove(parent_id)
                if depends_on:
                    job["depends_on"] = depends_on
                else:
                    job.pop("depends_on", None)
                    # Do not enqueue duplicates if the job is already runnable/in-flight.
                    current_state = job.get("status")
                    if current_state not in {State.DELAYED, State.ACTIVE, State.COMPLETED, State.FAILED, State.EXPIRED}:
                        job["status"] = State.WAITING
                        self.queues[queue_name] = [
                            queued
                            for queued in self.queues.get(queue_name, [])
                            if str(queued.get("id")) != str(job.get("id"))
                        ]
                        self.queues.setdefault(queue_name, []).append(job)
                        self.queues[queue_name].sort(key=lambda queued: queued.get("priority", 5))

                job_id = cast(str | None, job.get("id") or job.get("job_id"))
                if job_id:
                    await self._replace_lifecycle_payload_locked(queue_name, job)

    async def pause_queue(self, queue_name: str) -> None:
        """
        Asynchronously pauses processing for a specific queue.

        The pause flag is stored in MongoDB so all backend instances observe the
        same operator control state.

        Args:
            queue_name: The name of the queue to pause.
        """
        await self.connect()
        await self._queue_controls.update_one(
            {"queue_name": queue_name},
            {"$set": {"queue_name": queue_name, "paused": True, "paused_at": time.time()}},
            upsert=True,
        )
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Add the queue name to the in-memory set of paused queues.
            self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Asynchronously resumes processing for a specific queue.

        The persisted pause flag is removed so all backend instances can resume
        dequeueing work from the queue.

        Args:
            queue_name: The name of the queue to resume.
        """
        await self.connect()
        await self._queue_controls.delete_one({"queue_name": queue_name})
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Remove the queue name from the in-memory set of paused queues.
            # discard() is used instead of remove() to avoid raising a KeyError if the
            # queue was not paused.
            self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Asynchronously checks if a specific queue is currently paused.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue has a persisted pause flag, otherwise False.
        """
        await self.connect()
        doc = await self._queue_controls.find_one({"queue_name": queue_name})
        async with self.lock:
            if doc:
                self.paused.add(queue_name)
                return True
            self.paused.discard(queue_name)
            return False

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a running job in the MongoDB store.

        Loads the job data from the persistent store, adds or updates its 'progress'
        field, and saves the modified job data back to the store. This allows external
        monitoring of job progress. The lock is acquired for consistency.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: A float between 0.0 and 1.0 representing the job's progress.
        """
        # Acquire the lock for consistency, although primarily interacting with the store.
        async with self.lock:
            # Load the job data from the MongoDB store.
            job = await self.store.load(queue_name, job_id)
            # If the job data was successfully loaded.
            if job:
                job["progress"] = progress  # Add or update the progress field.
                await self.store.save(queue_name, job_id, job)  # Save the updated job data.

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue
        in a single batch operation.

        Adds the job payloads to the in-memory queue list for the given queue name
        and saves each job individually to the MongoDB store with its status set
        to `State.WAITING`. While the in-memory update is a batch operation,
        the store saves are individual. Access to the in-memory queue is synchronized
        using the internal lock.

        Args:
            queue_name: The name of the queue to enqueue jobs onto.
            jobs: A list of job payloads (dictionaries) to be enqueued. Each
                  dictionary is expected to contain at least an "id" key.
        """
        # Acquire the lock to ensure exclusive access to in-memory queues.
        async with self.lock:
            # Extend the in-memory queue list with the list of jobs.
            # setdefault ensures the list exists even if this is the first job for this queue.
            queue = self.queues.setdefault(queue_name, [])
            queue.extend(jobs)
            queue.sort(key=lambda job: job.get("priority", 5))
            # Iterate through each job and save it to the MongoDB store with the WAITING status.
            for job in jobs:
                await self.store.save(queue_name, job["id"], {**job, "status": State.WAITING})

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Asynchronously removes jobs from the MongoDB store based on their state
        and optional age criteria.

        Retrieves jobs matching the specified state from the persistent store.
        If `older_than` is provided, it filters these jobs and deletes only those
        whose relevant timestamp (e.g., completion time) is older than the
        specified value. If `older_than` is None, all jobs in the specified state
        are purged from the store. The lock is acquired for consistency.

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state of the jobs to target for purging (e.g., 'COMPLETED', 'FAILED').
            older_than: An optional timestamp (float). If provided, only jobs
                        whose relevant timestamp (e.g., 'completed_at') is before
                        this timestamp will be purged. If None, all jobs in the
                        specified state are purged from the store. Defaults to None.
        """
        await self._purge_jobs_by_state(queue_name, state, older_than)

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Asynchronously emits a backend-specific lifecycle event using the global
        event emitter.

        This allows the MongoDB backend to signal events (e.g., 'job_started',
        'job_completed') which can be used by other parts of the system for
        monitoring, logging, or UI updates.

        Args:
            event: A string representing the name of the event to emit.
            data: A dictionary containing data associated with the event.
        """
        # Emit the event using the core event emitter.
        await event_emitter.emit(event, data)

    async def create_lock(self, key: str, ttl: int) -> anyio.Lock:
        """
        Asynchronously creates and returns a backend-specific distributed lock.

        This method provides a mechanism for ensuring that only one worker or
        process can acquire a lock for a given key across a distributed system.
        In this MongoDB backend, it returns an `anyio.Lock` instance, which
        provides synchronization within the current process or thread group
        using `anyio`, but *not* a true distributed lock across multiple processes
        or machines. For true distributed locking with MongoDB, a different
        approach (e.g., using MongoDB's transactions or a dedicated locking collection)
        would be needed. The `key` and `ttl` arguments are part of the `BaseBackend`
        interface but are not directly used by the `anyio.Lock` itself.

        Args:
            key: A unique string identifier for the lock (not used by anyio.Lock).
            ttl: The time-to-live (in seconds) for the lock (not used by anyio.Lock).

        Returns:
            An `anyio.Lock` instance.
        """
        # Reuse a keyed lock so concurrent operations in this process serialize
        # on the same resource even though MongoDB does not currently provide a
        # distributed lock implementation here.
        if key not in self._locks:
            self._locks[key] = anyio.Lock()
        return self._locks[key]

    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> list[str]:
        """
        Atomically enqueues multiple jobs and registers their dependencies in the
        in-memory queue and MongoDB store.

        This method enqueues jobs into the in-memory queue and saves them to the
        MongoDB store. It then registers dependencies by loading each child job
        from the store, updating its 'depends_on' list, and saving it back.
        The entire operation is made atomic with respect to other operations on
        this backend instance by acquiring the internal lock.

        Args:
            queue_name: The target queue for all jobs in the flow.
            job_dicts: A list of job payloads (dictionaries) to enqueue.
            dependency_links: A list of tuples, where each tuple is
                              (parent_job_id, child_job_id), defining the
                              dependencies within the flow.

        Returns:
            A list of job IDs that were successfully enqueued, in the order
            they were provided in `job_dicts`.
        """
        # Acquire the lock to make the entire flow addition operation atomic within this instance.
        async with self.lock:
            created_ids: list[str] = []
            deps_by_child: dict[str, set[str]] = {}
            for parent, child in dependency_links:
                deps_by_child.setdefault(child, set()).add(parent)

            for payload in job_dicts:
                job_id = str(payload["id"])
                deps = set(payload.get("depends_on", [])) | deps_by_child.get(job_id, set())
                stored = {**payload, "status": State.WAITING}
                if deps:
                    stored["depends_on"] = sorted(deps)
                else:
                    stored.pop("depends_on", None)

                if not has_pending_dependencies(stored):
                    self.queues.setdefault(queue_name, []).append(stored)
                await self.store.save(queue_name, job_id, stored)
                created_ids.append(job_id)

            self.queues.setdefault(queue_name, []).sort(key=lambda job: job.get("priority", 5))

            return created_ids  # Return the list of IDs for the enqueued jobs.

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Asynchronously records or updates the last heartbeat timestamp for a
        specific active job.

        The timestamp is stored both in the process-local mirror and the
        MongoDB job document so a separate recovery process can detect stale
        active jobs after the original worker process exits.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            timestamp: The Unix timestamp (float) representing the time of the heartbeat.
        """
        # Acquire the lock to protect the shared heartbeats dictionary.
        async with self.lock:
            # Store the heartbeat timestamp using a tuple of queue name and job ID as the key.
            self.heartbeats[(queue_name, job_id)] = timestamp
            job = await self.store.load(queue_name, job_id)
            if job:
                job["heartbeat"] = timestamp
                await self.store.save(queue_name, job_id, job)

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves jobs whose last recorded heartbeat is older
        than a specified timestamp, indicating they might be stalled.

        The method checks both the process-local heartbeat mirror and persisted
        MongoDB job documents, so a separate recovery process can detect active
        jobs abandoned by a stopped worker process.

        Args:
            older_than: A Unix timestamp (float). Jobs with a heartbeat timestamp
                        strictly less than this value will be considered stalled.

        Returns:
            A list of dictionaries. Each dictionary contains 'queue_name' and
            'job_data' for the stalled jobs found. 'job_data' is the dictionary
            payload of the job.
        """
        stalled: list[dict[str, Any]] = []  # Initialize a list to store stalled job details
        seen: set[tuple[str, str]] = set()

        # Acquire the lock to protect shared state (heartbeats and queues accessed here).
        async with self.lock:
            # Iterate over a copy of heartbeats items to avoid issues if heartbeats
            # were modified during iteration.
            for (q, jid), ts in list(self.heartbeats.items()):
                # Check if the heartbeat timestamp is older than the threshold.
                if ts < older_than:
                    payload = await self.store.load(q, jid)
                    if payload:
                        stalled.append({"queue_name": q, "job_data": payload})
                        seen.add((q, jid))

            for job in await self.store.collection.find(
                {
                    "status": State.ACTIVE,
                    "$or": [
                        {"heartbeat": {"$lt": older_than}},
                        {
                            "heartbeat": {"$exists": False},
                            "$or": [
                                {"active_since": {"$lt": older_than}},
                                {"updated_at": {"$lt": older_than}},
                            ],
                        },
                    ],
                }
            ).to_list(length=None):
                queue = job.get("queue_name")
                job_id = job.get("job_id") or job.get("id")
                if not isinstance(queue, str) or not isinstance(job_id, str):
                    continue
                if (queue, job_id) in seen:
                    continue
                job.pop("_id", None)
                stalled.append({"queue_name": queue, "job_data": job})
                seen.add((queue, job_id))

        return stalled

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        """
        Asynchronously re-enqueues a stalled job back onto its waiting queue in
        memory and removes its heartbeat entry.

        This method appends the job data back to the end of the specified in-memory
        queue list and removes the job's entry from the in-memory heartbeats
        dictionary. Access to both queues and heartbeats is synchronized using
        the internal lock.

        Args:
            queue_name: The name of the queue the job should be re-enqueued into.
            job_data: The dictionary containing the job's data, which is appended
                      to the queue. Must contain an 'id' key for heartbeat removal.
        """
        await self.connect()
        retry_payload = dict(job_data)
        retry_payload.pop("_id", None)
        retry_payload.pop("job_id", None)
        retry_payload.pop("queue_name", None)
        retry_payload.pop("status", None)
        retry_payload.pop("heartbeat", None)
        retry_payload.pop("updated_at", None)
        expected_active_since = retry_payload.pop("active_since", None)
        job_id = retry_payload.get("id")
        if not isinstance(job_id, str):
            return
        if await self.is_job_cancelled(queue_name, job_id):
            return

        query: dict[str, Any] = {"queue_name": queue_name, "job_id": job_id, "status": State.ACTIVE}
        if isinstance(expected_active_since, (int, float)):
            query["active_since"] = {
                "$gte": float(expected_active_since) - 0.000001,
                "$lte": float(expected_active_since) + 0.000001,
            }

        now = time.time()
        waiting_payload = {**retry_payload, "status": State.WAITING, "updated_at": now}
        result = await self.store.collection.update_one(
            query,
            {
                "$set": {**waiting_payload, "queue_name": queue_name, "job_id": job_id},
                "$unset": {"heartbeat": "", "active_since": ""},
            },
        )
        if result.modified_count == 0:
            return

        async with self.lock:
            self.queues[queue_name] = [
                queued for queued in self.queues.get(queue_name, []) if str(queued.get("id")) != job_id
            ]
            self.queues.setdefault(queue_name, []).append(waiting_payload)
            self.queues[queue_name].sort(key=lambda job: job.get("priority", 5))
            self.heartbeats.pop((queue_name, job_id), None)

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Asynchronously provides the number of jobs currently in the waiting, delayed,
        and failed (DLQ) states for the given queue.

        Retrieves counts from the in-memory queues (waiting, delayed) and queries
        the persistent store for failed jobs. Calculates the waiting count by
        subtracting failed jobs from the raw in-memory count. Access to in-memory
        queues is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to get statistics for.

        Returns:
            A dictionary containing the counts for "waiting", "delayed", and
            "failed" jobs.
        """
        waiting = len(await self.store.jobs_by_status(queue_name, State.WAITING))
        delayed = len(await self.store.jobs_by_status(queue_name, State.DELAYED))
        failed = len(await self.store.jobs_by_status(queue_name, State.FAILED))
        return {"waiting": waiting, "delayed": delayed, "failed": failed}

    async def list_delayed(self, queue_name: str) -> list[DelayedInfo]:
        """
        Asynchronously retrieves a list of all currently delayed jobs for a
        specific queue from the in-memory delayed queue.

        Iterates through the in-memory delayed queue list, sorts the jobs by
        their scheduled run time (`run_at`), and converts them into a list
        of `DelayedInfo` dataclass instances. Access to the in-memory delayed
        queue is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to list delayed jobs for.

        Returns:
            A list of `DelayedInfo` dataclass instances.
        """
        # Acquire the lock to ensure exclusive access to in-memory delayed queues.
        async with self.lock:
            out: list[DelayedInfo] = []  # Initialize a list to store DelayedInfo instances.
            # Iterate through delayed jobs for the specified queue, sorted by run_at timestamp.
            for run_at, job in sorted(self.delayed.get(queue_name, []), key=lambda x: x[0]):
                # Append a new DelayedInfo instance to the output list.
                out.append(DelayedInfo(job_id=job["id"], run_at=run_at, payload=job))
            return out  # Return the list of DelayedInfo instances.

    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Asynchronously retrieves durable repeatable job definitions for a queue.

        Args:
            queue_name: The name of the queue to list repeatable jobs for.

        Returns:
            A list of `RepeatableInfo` dataclass instances.
        """
        rows = await self.store.jobs_by_status(queue_name, "repeatable")
        repeatables = [
            RepeatableInfo(
                job_def=dict(row["job_def"]),
                next_run=float(row["next_run"]),
                paused=bool(row.get("paused", False)),
            )
            for row in rows
        ]
        return sorted(repeatables, key=lambda item: item.next_run)

    async def upsert_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Persist a repeatable definition in MongoDB.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition.

        Returns:
            The next scheduled UNIX timestamp for the definition.
        """
        clean_def = normalize_repeatable_job_def(job_def)
        repeatable_id = repeatable_identity(clean_def)
        next_run = compute_next_run(clean_def)
        await self.store.save(
            queue_name,
            repeatable_id,
            {
                "id": repeatable_id,
                "job_def": clean_def,
                "next_run": next_run,
                "paused": False,
                "status": "repeatable",
            },
        )
        return next_run

    async def get_due_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Return durable repeatables whose next run is due.

        Args:
            queue_name: The queue to inspect.

        Returns:
            Due repeatable definitions ordered by next-run time.
        """
        rows = await self.store.jobs_by_status(queue_name, "repeatable")
        now = time.time()
        due = [
            RepeatableInfo(
                job_def=dict(row["job_def"]),
                next_run=float(row["next_run"]),
                paused=bool(row.get("paused", False)),
            )
            for row in rows
            if not row.get("paused", False) and float(row["next_run"]) <= now
        ]
        return sorted(due, key=lambda item: item.next_run)

    async def advance_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Advance a durable repeatable definition after one occurrence is emitted.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition that just fired.

        Returns:
            The new next-run UNIX timestamp.
        """
        clean_def = normalize_repeatable_job_def(job_def)
        repeatable_id = repeatable_identity(clean_def)
        next_run = compute_next_run(clean_def)
        current = await self.store.load(queue_name, repeatable_id) or {}
        await self.store.save(
            queue_name,
            repeatable_id,
            {
                **current,
                "id": repeatable_id,
                "job_def": clean_def,
                "next_run": next_run,
                "paused": False,
                "status": "repeatable",
            },
        )
        return next_run

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Pause a durable repeatable definition.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to pause.
        """
        repeatable_id = repeatable_identity(job_def)
        current = await self.store.load(queue_name, repeatable_id)
        if current is None:
            return
        current["paused"] = True
        await self.store.save(queue_name, repeatable_id, current)

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> Any:
        """
        Resume a paused durable repeatable definition.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to resume.

        Returns:
            The newly computed timestamp (float) for the next run of the repeatable job.

        Raises:
            KeyError: If the specified repeatable job definition is not found.
        """
        repeatable_id = repeatable_identity(job_def)
        current = await self.store.load(queue_name, repeatable_id)
        if current is None:
            raise KeyError(f"Repeatable job not found: {job_def}")
        clean_def = normalize_repeatable_job_def(current["job_def"])
        next_ts = compute_next_run(clean_def)
        await self.store.save(
            queue_name,
            repeatable_id,
            {
                **current,
                "id": repeatable_id,
                "job_def": clean_def,
                "next_run": next_ts,
                "paused": False,
                "status": "repeatable",
            },
        )
        return next_ts

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any] | str) -> None:
        """
        Remove a durable repeatable definition from MongoDB.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: Either the logical definition or the repeatable identifier.
        """
        repeatable_id = job_def if isinstance(job_def, str) else repeatable_identity(job_def)
        await self.store.delete(queue_name, repeatable_id)

    async def cancel_job(self, queue_name: str, job_id: str) -> bool:
        """
        Cancel a job durably so every MongoDB backend instance can observe it.

        The cancellation marker is stored in MongoDB, and runnable waiting,
        delayed, or active job documents are moved to ``cancelled`` status so
        other processes cannot claim them as executable work.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to cancel.
        """
        await self.connect()
        await self._mark_job_cancelled(queue_name, job_id)
        now = time.time()
        result = await self.store.collection.update_one(
            {
                "queue_name": queue_name,
                "job_id": job_id,
                "status": {"$in": [State.WAITING, State.DELAYED, State.ACTIVE]},
            },
            {"$set": {"status": "cancelled", "cancelled_at": now, "updated_at": now}},
        )

        async with self.lock:
            removed = result.matched_count > 0
            waiting_before = len(self.queues.get(queue_name, []))
            self.queues[queue_name] = [j for j in self.queues.get(queue_name, []) if j.get("id") != job_id]
            if len(self.queues[queue_name]) < waiting_before:
                removed = True
            delayed_before = len(self.delayed.get(queue_name, []))
            self.delayed[queue_name] = [(ts, j) for ts, j in self.delayed.get(queue_name, []) if j.get("id") != job_id]
            if len(self.delayed[queue_name]) < delayed_before:
                removed = True
            self.cancelled.setdefault(queue_name, set()).add(job_id)
            self.heartbeats.pop((queue_name, job_id), None)
            return removed

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
        """
        Check whether a job has been marked as cancelled.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to check.

        Returns:
            True if the job ID is found in the durable cancellation marker
            collection or local cache for the specified queue, False otherwise.
        """
        await self.connect()
        if await self._is_job_durably_cancelled(queue_name, job_id):
            return True
        async with self.lock:
            return job_id in self.cancelled.get(queue_name, set())

    async def list_jobs(self, queue: str, state: str) -> list[dict[str, Any]]:
        normalized = normalize_job_type(state)

        if normalized == "waiting-children":
            jobs = await self.store.all_jobs(queue)
            cleaned: list[dict[str, Any]] = []
            for job in jobs:
                job = dict(job)
                job.pop("_id", None)
                if matches_job_type(job, normalized):
                    cleaned.append(job)
            return cleaned

        if normalized in {"paused", "prioritized"}:
            return []

        jobs = await self.store.filter(queue=queue, state=normalized)
        return [job for job in jobs if matches_job_type(job, normalized)]

    async def list_queues(self) -> list[str]:
        """
        Return all unique queue names present in the job collection in MongoDB.

        Connects to the MongoDB store and retrieves a list of all distinct
        values in the "queue_name" field across all documents in the job collection.

        Returns:
            A list of strings, where each string is a unique queue name.
        """
        # Ensure the store is connected
        await self.store.connect()
        # Use MongoDB distinct to get unique queue names
        queues: list[str] = await self.store.collection.distinct("queue_name")
        return queues

    async def register_worker(self, worker_id: str, queue: str, concurrency: int, timestamp: float) -> None:
        """
        Register or update a worker's heartbeat in the MongoDB backend.

        Inserts a new worker document or updates an existing one in the
        workers collection based on the worker_id (_id field). It stores
        the worker's assigned queue, concurrency level, and the current
        heartbeat timestamp. Uses upsert=True for creation or update.

        Args:
            worker_id: The unique identifier for the worker.
            queue: The name of the queue the worker is associated with.
            concurrency: The concurrency level of the worker.
            timestamp: The timestamp representing the worker's last heartbeat.
        """
        await self._workers.update_one(
            {"_id": worker_id},
            {"$set": {"queue": queue, "concurrency": concurrency, "heartbeat": timestamp}},
            upsert=True,
        )

    async def deregister_worker(self, worker_id: str) -> None:
        """
        Remove a worker's registry entry from the MongoDB backend.

        Deletes the document corresponding to the specified worker_id (_id field)
        from the workers collection.

        Args:
            worker_id: The unique identifier of the worker to deregister.
        """
        await self._workers.delete_one({"_id": worker_id})

    async def list_workers(self) -> list[WorkerInfo]:
        """
        Lists active workers from the MongoDB backend.

        Retrieves workers from the workers collection whose last heartbeat
        is within the configured time-to-live (TTL).

        Returns:
            A list of WorkerInfo objects representing the active workers.
        """
        cutoff = time.time() - self._settings.heartbeat_ttl
        cursor = self._workers.find({"heartbeat": {"$gte": cutoff}})
        workers = []
        async for doc in cursor:
            workers.append(
                WorkerInfo(
                    id=doc["_id"],
                    queue=doc.get("queue", ""),
                    concurrency=doc.get("concurrency", 1),
                    heartbeat=doc["heartbeat"],
                )
            )
        return workers

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        """
        Retry a job by moving it from DLQ or failed state back to waiting.

        Loads the job document from storage, updates its status to WAITING,
        and persists the changes. This makes the job available for processing again.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
        """
        job = await self.store.load(queue_name, job_id)
        if not job:
            return False

        retry_payload = self._prepare_retry_payload(job, job_id)
        retry_payload.pop("_id", None)
        async with self.lock:
            self.queues[queue_name] = [j for j in self.queues.get(queue_name, []) if str(j.get("id")) != job_id]
            self.delayed[queue_name] = [
                (run_at, j) for run_at, j in self.delayed.get(queue_name, []) if str(j.get("id")) != job_id
            ]
            queue = self.queues.setdefault(queue_name, [])
            queue.append(retry_payload)
            queue.sort(key=lambda item: item.get("priority", 5))
            document = {**retry_payload, "queue_name": queue_name, "job_id": job_id}
            await self.store.collection.replace_one(
                {"queue_name": queue_name, "job_id": job_id},
                document,
                upsert=True,
            )
        return True

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        """
        Remove a job completely from storage.

        Deletes the job document with the specified ID from the given queue.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove.
        """
        await self.connect()
        existing = await self.store.load(queue_name, job_id)
        cancelled = await self._is_job_durably_cancelled(queue_name, job_id)
        if not existing and not cancelled:
            return False
        if existing:
            await self.store.delete(queue_name, job_id)
        await self._clear_job_cancelled(queue_name, job_id)
        async with self.lock:
            self.queues[queue_name] = [j for j in self.queues.get(queue_name, []) if j.get("id") != job_id]
            self.delayed[queue_name] = [(ts, j) for ts, j in self.delayed.get(queue_name, []) if j.get("id") != job_id]
            self.heartbeats.pop((queue_name, job_id), None)
            self.cancelled.get(queue_name, set()).discard(job_id)
        return True
