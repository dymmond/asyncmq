import time
from typing import Any

import anyio

# Conditional import for motor, handled with a try/except block
try:
    import motor  # noqa
except ImportError:
    # If motor is not installed, raise a specific ImportError.
    raise ImportError("Please install motor: `pip install motor`") from None

# Import necessary components from asyncmq.
from asyncmq.backends.base import BaseBackend
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.stores.mongodb import MongoDBStore


class MongoDBBackend(BaseBackend):
    """
    MongoDB backend for AsyncMQ.

    Manages job queues, delayed jobs, and job states using a MongoDBStore
    for persistent storage and mongodb structures for active and delayed
    jobs. Uses anyio locks for thread-safe access to mongodb queues.
    Requires calling the `connect` async method after initialization.
    """

    def __init__(self, mongo_url: str = "mongodb://localhost", database: str = "asyncmq") -> None:
        """
        Initializes the MongoDB backend and its underlying store.

        Creates an instance of the MongoDBStore for persistent storage and
        initializes mongodb datastructures for queues, delayed jobs, and
        paused queues, along with an anyio lock for synchronization.

        Args:
            mongo_url: The MongoDB connection URL. Defaults to "mongodb://localhost".
            database: The name of the MongoDB database to use. Defaults to "asyncmq".
        """
        # Initialize the MongoDB store for persistent job data.
        self.store = MongoDBStore(mongo_url, database)
        # mongodb queue: maps queue names to lists of waiting jobs.
        self.queues: dict[str, list[dict[str, Any]]] = {}
        # mongodb delayed queue: maps queue names to lists of (run_at, job) tuples.
        self.delayed: dict[str, list[tuple[float, dict[str, Any]]]] = {}
        # Set of queue names that are currently paused.
        self.paused: set[str] = set()
        # Lock for synchronizing access to mongodb queues.
        self.lock = anyio.Lock()

    async def connect(self) -> None:
        """
        Connects the backend to the MongoDB database and initializes indexes.

        This async method must be called after creating the backend instance
        to establish the connection and ensure that the necessary indexes
        are created in the MongoDB store.
        """
        # Call the connect method of the underlying MongoDBStore.
        await self.store.connect()

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Adds a job to the specified queue.

        Adds the job payload to the mongodb queue and saves the job to the
        MongoDB store with its status set to WAITING.

        Args:
            queue_name: The name of the queue to add the job to.
            payload: A dictionary containing the job data, including an 'id'.
        """
        async with self.lock:
            # Add the payload to the end of the mongodb queue list for this queue.
            self.queues.setdefault(queue_name, []).append(payload)
            # Save the job to the MongoDB store with the WAITING status.
            await self.store.save(queue_name, payload["id"], {**payload, "status": State.WAITING})

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Retrieves the next available job from the specified queue.

        Pops the first job from the mongodb queue (if available), updates its
        status to ACTIVE in the MongoDB store, and returns the job.

        Args:
            queue_name: The name of the queue to dequeue from.

        Returns:
            The job dictionary if a job was available, otherwise None.
        """
        async with self.lock:
            # Get the mongodb queue list for the specified queue name, defaulting to empty.
            queue = self.queues.get(queue_name, [])
            if queue:
                # Remove and return the first job from the list.
                job = queue.pop(0)
                # Update the job's status to ACTIVE.
                job["status"] = State.ACTIVE
                # Save the updated job state to the MongoDB store.
                await self.store.save(queue_name, job["id"], job)
                return job
            # Return None if the queue is empty.
            return None

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Marks a job as failed and moves it to the Dead-Letter Queue (DLQ).

        Updates the job's status to FAILED in the MongoDB store.

        Args:
            queue_name: The name of the queue the job originally belonged to.
            payload: A dictionary containing the job data, including an 'id'.
        """
        async with self.lock:
            # Update the job's status to FAILED.
            payload["status"] = State.FAILED
            # Save the updated job state to the MongoDB store.
            await self.store.save(queue_name, payload["id"], payload)

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Acknowledges a job's successful processing.

        For this backend, state updates (like completion or failure) are handled
        by `update_job_state`, so explicit acknowledgment here is a no-op.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The ID of the acknowledged job.
        """
        # Nothing required here; completion/failure state is updated elsewhere.
        pass

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Adds a job to the delayed queue to be processed at a later time.

        Adds the job payload and its scheduled run time to the mongodb delayed
        queue and saves the job to the MongoDB store with its status set to EXPIRED.

        Args:
            queue_name: The name of the queue the job will eventually be added to.
            payload: A dictionary containing the job data, including an 'id'.
            run_at: A timestamp (float) indicating when the job should be run.
        """
        async with self.lock:
            # Add the job and its scheduled run time to the mongodb delayed list.
            self.delayed.setdefault(queue_name, []).append((run_at, payload))
            # Save the job to the MongoDB store with the EXPIRED status.
            await self.store.save(queue_name, payload["id"], {**payload, "status": State.EXPIRED})

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Retrieves delayed jobs from the specified queue that are due to run now.

        Checks the mongodb delayed queue for jobs whose scheduled run time
        is less than or equal to the current time. Removes due jobs from the
        delayed queue and returns them.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of job dictionaries that are ready to be processed.
        """
        async with self.lock:
            # Get the current time.
            now = time.time()
            due = []  # List to store jobs that are due.
            remaining = []  # List to store jobs that are not yet due.
            # Iterate through delayed jobs for the specified queue.
            for run_at, job in self.delayed.get(queue_name, []):
                # Check if the job's run time is now or in the past.
                if run_at <= now:
                    due.append(job)  # Add due jobs to the 'due' list.
                else:
                    remaining.append((run_at, job))  # Add remaining jobs to 'remaining'.
            # Update the mongodb delayed list with only the remaining jobs.
            self.delayed[queue_name] = remaining
            return due  # Return the list of jobs that were due.

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Removes a job from the mongodb delayed queue by its ID.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The ID of the job to remove.
        """
        async with self.lock:
            # Filter the mongodb delayed list, keeping only jobs whose ID does
            # not match the one to be removed.
            self.delayed[queue_name] = [
                (ts, job) for ts, job in self.delayed.get(queue_name, []) if job.get("id") != job_id
            ]

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Updates the state of a job in the MongoDB store.

        Loads the job from the store, updates its 'status' field, and saves it
        back to the store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The ID of the job to update.
            state: The new state string (e.g., 'COMPLETED', 'FAILED').
        """
        # Load the job from the store.
        job = await self.store.load(queue_name, job_id)
        # If the job is found, update its status and save it.
        if job:
            job["status"] = state  # Update the job's status.
            await self.store.save(queue_name, job_id, job)  # Save the updated job.

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Saves the result of a completed job in the MongoDB store.

        Loads the job from the store, adds/updates its 'result' field, and
        saves it back to the store.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The ID of the job whose result is to be saved.
            result: The result data of the job (can be any JSON-serializable type).
        """
        # Load the job from the store.
        job = await self.store.load(queue_name, job_id)
        # If the job is found, add/update the result and save it.
        if job:
            job["result"] = result  # Add or update the result field.
            await self.store.save(queue_name, job_id, job)  # Save the updated job.

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Retrieves the current state of a job from the MongoDB store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The ID of the job whose state is to be retrieved.

        Returns:
            The job's state string if the job is found, otherwise None.
        """
        # Load the job from the store.
        job = await self.store.load(queue_name, job_id)
        # Return the 'status' field if the job is found, otherwise None.
        return job.get("status") if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Retrieves the result of a job from the MongoDB store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The ID of the job whose result is to be retrieved.

        Returns:
            The job's result data if the job is found and has a result,
            otherwise None.
        """
        # Load the job from the store.
        job = await self.store.load(queue_name, job_id)
        # Return the 'result' field if the job is found, otherwise None.
        return job.get("result") if job else None

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Adds dependency information to a job.

        This functionality is not currently implemented for the MongoDB backend.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The dictionary representing the job.
        """
        # This functionality is not currently implemented for the MongoDB backend.
        pass

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Resolves a dependency for jobs waiting on the specified parent job.

        This functionality is not currently implemented for the MongoDB backend.

        Args:
            queue_name: The name of the queue containing the dependent jobs.
            parent_id: The ID of the parent job whose dependency is resolved.
        """
        # This functionality is not currently implemented for the MongoDB backend.
        pass

    async def pause_queue(self, queue_name: str) -> None:
        """
        Pauses processing for a specific queue.

        Adds the queue name to the set of paused queues, preventing workers
        from dequeueing new jobs from it.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Add the queue name to the mongodb set of paused queues.
        self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Resumes processing for a specific queue.

        Removes the queue name from the set of paused queues, allowing workers
        to resume dequeueing jobs from it.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Remove the queue name from the mongodb set of paused queues.
        self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Checks if a specific queue is currently paused.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue is paused, False otherwise.
        """
        # Check if the queue name is present in the mongodb set of paused queues.
        return queue_name in self.paused

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Saves the progress of a running job in the MongoDB store.

        Loads the job from the store, adds/updates its 'progress' field, and
        saves it back to the store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The ID of the job whose progress is to be saved.
            progress: A float representing the job's progress (e.g., 0.0 to 1.0).
        """
        # Load the job from the store.
        job = await self.store.load(queue_name, job_id)
        # If the job is found, add/update the progress and save it.
        if job:
            job["progress"] = progress  # Add or update the progress field.
            await self.store.save(queue_name, job_id, job)  # Save the updated job.

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Adds multiple jobs to the specified queue in a bulk operation.

        Adds the job payloads to the mongodb queue and saves each job to the
        MongoDB store with its status set to WAITING.

        Args:
            queue_name: The name of the queue to add the jobs to.
            jobs: A list of job dictionaries to enqueue.
        """
        async with self.lock:
            # Extend the mongodb queue list with the list of jobs.
            self.queues.setdefault(queue_name, []).extend(jobs)
            # Iterate through each job and save it to the MongoDB store.
            for job in jobs:
                await self.store.save(queue_name, job["id"], {**job, "status": State.WAITING})

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Removes jobs from the store based on their state and age.

        Retrieves jobs matching the specified state from the MongoDB store and
        deletes them if they are older than the provided timestamp (if any).

        Args:
            queue_name: The name of the queue to purge jobs from.
            state: The state of the jobs to target for purging (e.g., 'COMPLETED').
            older_than: An optional timestamp. Only jobs completed before this
                        time will be purged. If None, all jobs in the specified
                        state are purged.
        """
        # Get jobs from the store matching the specified state.
        jobs = await self.store.jobs_by_status(queue_name, state)
        now = time.time()  # Get the current time for age comparison if needed.
        # Iterate through the retrieved jobs.
        for job in jobs:
            # Get the completion timestamp, defaulting to 'now' if not available
            # (e.g., for non-completed states).
            ts = job.get("completed_at", now)
            # Check if older_than is None (purge all) or if the job's timestamp
            # is before the older_than timestamp.
            if older_than is None or ts < older_than:
                # Delete the job from the store.
                await self.store.delete(queue_name, job["id"])

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Emits a backend-specific event.

        Uses the global event emitter to broadcast an event with associated data.

        Args:
            event: The name of the event to emit.
            data: A dictionary containing the event data.
        """
        # Emit the event using the core event emitter.
        await event_emitter.emit(event, data)

    async def create_lock(self, key: str, ttl: int) -> anyio.Lock:
        """
        Creates a backend-specific lock.

        Returns an anyio Lock for synchronization within the backend.

        Args:
            key: A string key for the lock (used in some backends, but not
                 explicitly by the anyio.Lock itself).
            ttl: The time-to-live for the lock in seconds (not directly used
                 by anyio.Lock but part of the backend interface).

        Returns:
            An anyio.Lock instance.
        """
        # Return a standard anyio.Lock instance.
        return anyio.Lock()

    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> list[str]:
        """
        Atomically enqueue multiple jobs and register dependencies mongodb and
        store.

        Enqueues jobs into the mongodb queue and saves them to the MongoDB
        store. Registers dependencies by loading the child job from the store,
        updating its 'depends_on' list, and saving it back. This method is
        atomic with respect to other operations on this backend instance due
        to the use of the internal lock.

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
        async with self.lock:
            created_ids: list[str] = []
            # Enqueue payloads into the mongodb queue and save to the store.
            for payload in job_dicts:
                # Add the payload to the mongodb queue list.
                self.queues.setdefault(queue_name, []).append(payload)
                # Save the job to the MongoDB store with the WAITING status.
                await self.store.save(queue_name, payload["id"], {**payload, "status": State.WAITING})
                # Add the job ID to the list of created IDs.
                created_ids.append(payload["id"])

            # Register dependencies by updating the 'depends_on' field in the store.
            for parent, child in dependency_links:
                # Load the child job from the store.
                job = await self.store.load(queue_name, child)
                # If the child job is found.
                if job:
                    # Get the existing dependencies list, defaulting to empty.
                    deps = job.get("depends_on", [])
                    # If the parent is not already in the dependencies list.
                    if parent not in deps:
                        # Add the parent to the dependencies list.
                        deps.append(parent)
                        # Update the 'depends_on' field in the job data.
                        job["depends_on"] = deps
                        # Save the updated job data back to the store.
                        await self.store.save(queue_name, child, job)

            return created_ids  # Return the list of IDs for the enqueued jobs.

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        await self.connect()
        # embed heartbeat into the stored document
        await self.store.collection.update_one(
            {"queue": queue_name, "id": job_id},
            {"$set": {"data.heartbeat": timestamp}}
        )

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        await self.connect()
        stalled: list[dict[str, Any]] = []
        # for each queue, find ACTIVE jobs with stale heartbeat
        async for queue_name in self.queues.keys():
            docs = await self.store.collection.find(
                {"queue": queue_name, "data.status": State.ACTIVE, "data.heartbeat": {"$lt": older_than}}
            ).to_list(None)
            for doc in docs:
                stalled.append({"queue_name": queue_name, "job_data": doc["data"]})
        return stalled

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        # re-enqueue via normal path
        await self.enqueue(queue_name, job_data)
        # optionally clear heartbeat field
        await self.store.collection.update_one(
            {"queue": queue_name, "id": job_data["id"]},
            {"$unset": {"data.heartbeat": ""}}
        )
