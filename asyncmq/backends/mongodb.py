import json
import time
from typing import Any, cast

import anyio

# Conditional import for motor, handled with a try/except block.
# Motor is an asynchronous MongoDB driver.
try:
    import motor  # noqa: F401 # Import motor, ignore unused warning if not directly called in this file
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
from asyncmq.conf import monkay
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
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
        # In-memory repeatable definitions: queue_name -> { raw_json_def -> { job_def, next_run, paused } }
        self.repeatables: dict[str, dict[str, dict[str, Any]]] = {}
        # In-memory cancelled-job sets: queue_name -> set(job_id). Stores IDs of cancelled jobs.
        self.cancelled: dict[str, set[str]] = {}
        # An anyio Lock used to synchronize access to the in-memory data structures.
        self.lock: anyio.Lock = anyio.Lock()
        # In-memory heartbeats: maps (queue_name, job_id) tuples to their last heartbeat timestamp.
        self.heartbeats: dict[tuple[str, str], float] = {}
        self._workers = self.store.db["worker_heartbeats"]

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

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
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
            # Add the payload to the end of the in-memory queue list for this queue.
            # setdefault ensures the list exists even if this is the first job for this queue.
            self.queues.setdefault(queue_name, []).append(payload)
            # Save the job to the MongoDB store with the WAITING status.
            # Use {**payload, "status": State.WAITING} to create a new dict with updated status.
            await self.store.save(queue_name, payload["id"], {**payload, "status": State.WAITING})

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
        # Acquire the lock to ensure exclusive access to in-memory queues.
        async with self.lock:
            # Get the in-memory queue list for the specified queue name, defaulting to an empty list.
            queue = self.queues.get(queue_name, [])
            # Check if the queue list is not empty.
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
        # Acquire the lock for consistency, although only the store is modified here.
        async with self.lock:
            # Update the job's status to FAILED.
            payload["status"] = State.FAILED
            # Save the updated job state to the MongoDB store.
            await self.store.save(queue_name, payload["id"], payload)

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
            self.delayed.setdefault(queue_name, []).append((run_at, payload))
            # Save the job to the MongoDB store with the EXPIRED status.
            # Note: The original code sets status to EXPIRED here. Consider changing to DELAYED
            # if a distinct DELAYED state is desired in the store.
            await self.store.save(queue_name, payload["id"], {**payload, "status": State.DELAYED})

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
        # Acquire the lock to ensure exclusive access to in-memory delayed queues.
        async with self.lock:
            # Get the current time.
            now = time.time()
            due: list[dict[str, Any]] = []  # list to store jobs that are due.
            remaining: list[tuple[float, dict[str, Any]]] = []  # list to store jobs not yet due.
            # Iterate through delayed jobs for the specified queue. Use .get() with default []
            # to handle cases where the queue name doesn't exist in self.delayed.
            for run_at, job in self.delayed.get(queue_name, []):
                # Check if the job's run time is now or in the past.
                if run_at <= now:
                    due.append(job)  # Add due jobs to the 'due' list.
                else:
                    remaining.append((run_at, job))  # Add remaining jobs to 'remaining'.
            # Update the in-memory delayed list for this queue with only the remaining jobs.
            self.delayed[queue_name] = remaining
            return due  # Return the list of jobs that were due.

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
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
        # Acquire the lock to ensure exclusive access to in-memory delayed queues.
        async with self.lock:
            # Filter the in-memory delayed list, keeping only jobs whose ID does
            # not match the one to be removed. Use .get() with default [] for safety.
            self.delayed[queue_name] = [
                (ts, job) for ts, job in self.delayed.get(queue_name, []) if job.get("id") != job_id
            ]

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
        # Acquire the lock for consistency.
        async with self.lock:
            # Find jobs in the store that have 'parent_id' in their 'depends_on' list.
            # This requires a store method to query by dependency. Assuming such a method exists.
            # NOTE: The original code did not implement this query logic here.
            # This is a placeholder assuming the store has a method like `jobs_depending_on`.
            # As per instructions, I cannot change the logic, so the original pass is kept,
            # but this is where the dependency resolution would typically happen.
            # Placeholder for actual dependency resolution logic:
            # dependent_jobs = await self.store.jobs_depending_on(queue_name, parent_id)
            # for child_job in dependent_jobs:
            #     child_job['depends_on'].remove(parent_id)
            #     if not child_job['depends_on']:
            #         child_job['status'] = State.WAITING
            #         await self.store.save(queue_name, child_job['id'], child_job)
            #         self.queues.setdefault(queue_name, []).append(child_job)
            #     else:
            #         await self.store.save(queue_name, child_job['id'], child_job)
            pass  # Keeping the original pass statement as per instruction

    async def pause_queue(self, queue_name: str) -> None:
        """
        Asynchronously pauses processing for a specific queue by marking it in memory.

        Adds the queue name to the in-memory set of paused queues. Workers checking
        this set should stop dequeueing new jobs from this queue. Access to the
        in-memory paused set is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Add the queue name to the in-memory set of paused queues.
            self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Asynchronously resumes processing for a specific queue by unmarking it in memory.

        Removes the queue name from the in-memory set of paused queues, allowing
        workers to resume dequeueing jobs from it. Access to the in-memory paused
        set is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Remove the queue name from the in-memory set of paused queues.
            # discard() is used instead of remove() to avoid raising a KeyError if the
            # queue was not paused.
            self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Asynchronously checks if a specific queue is currently marked as paused in memory.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue name is present in the in-memory set of paused queues,
            False otherwise. Access to the in-memory paused set is synchronized
            using the internal lock.
        """
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Check if the queue name is present in the in-memory set of paused queues.
            return queue_name in self.paused

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
            self.queues.setdefault(queue_name, []).extend(jobs)
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
        # Acquire the lock for consistency.
        async with self.lock:
            # Get jobs from the store matching the specified state.
            jobs = await self.store.jobs_by_status(queue_name, state)
            now = time.time()  # Get the current time for age comparison if needed.
            # Iterate through the retrieved jobs.
            for job in jobs:
                # Determine the relevant timestamp for comparison. Using 'completed_at'
                # as a common timestamp for states like COMPLETED or FAILED, defaulting
                # to 'now' if not available (e.g., for states like WAITING if purging them).
                # NOTE: This logic assumes 'completed_at' is the relevant timestamp for purging.
                # More robust logic might consider other timestamps based on the 'state' argument.
                ts = job.get("completed_at", now)
                # Check if older_than is None (purge all in state) or if the job's timestamp
                # is strictly before the older_than timestamp.
                if older_than is None or ts < older_than:
                    # Delete the job from the store.
                    await self.store.delete(queue_name, job["id"])

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
        # Return a standard anyio.Lock instance. This provides local concurrency control.
        return anyio.Lock()

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
            # Enqueue payloads into the in-memory queue and save to the store.
            for payload in job_dicts:
                # Add the payload to the in-memory queue list for this queue.
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
                    # Get the existing dependencies list from the loaded job, defaulting to empty.
                    deps = job.get("depends_on", [])
                    # If the parent ID is not already in the dependencies list.
                    if parent not in deps:
                        # Add the parent ID to the dependencies list.
                        deps.append(parent)
                        # Update the 'depends_on' field in the job data.
                        job["depends_on"] = deps
                        # Save the updated job data back to the store.
                        await self.store.save(queue_name, child, job)

            return created_ids  # Return the list of IDs for the enqueued jobs.

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Asynchronously records or updates the last heartbeat timestamp for a
        specific job in the in-memory heartbeats dictionary.

        This method acquires a lock to ensure safe concurrent access to the
        heartbeats dictionary and stores the provided timestamp associated with
        the job's queue name and ID. This is used by the stalled job detection
        mechanism.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            timestamp: The Unix timestamp (float) representing the time of the heartbeat.
        """
        # Acquire the lock to protect the shared heartbeats dictionary.
        async with self.lock:
            # Store the heartbeat timestamp using a tuple of queue name and job ID as the key.
            self.heartbeats[(queue_name, job_id)] = timestamp

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves jobs whose last recorded heartbeat in memory is
        older than a specified timestamp, indicating they might be stalled.

        This method iterates through the in-memory heartbeats, identifies jobs whose
        heartbeat timestamp is less than `older_than`, and then attempts to find
        the corresponding job data in the in-memory queues. Access to both heartbeats
        and queues is synchronized using the internal lock.

        Args:
            older_than: A Unix timestamp (float). Jobs with a heartbeat timestamp
                        strictly less than this value will be considered stalled.

        Returns:
            A list of dictionaries. Each dictionary contains 'queue_name' and
            'job_data' for the stalled jobs found. 'job_data' is the dictionary
            payload of the job.
        """
        stalled: list[dict[str, Any]] = []  # Initialize a list to store stalled job details

        # Acquire the lock to protect shared state (heartbeats and queues accessed here).
        async with self.lock:
            # Iterate over a copy of heartbeats items to avoid issues if heartbeats
            # were modified during iteration.
            for (q, jid), ts in list(self.heartbeats.items()):
                # Check if the heartbeat timestamp is older than the threshold.
                if ts < older_than:
                    # Attempt to find the full job payload in the relevant in-memory queue
                    # using a generator expression for efficiency. Use .get("id")
                    # for safety in case 'id' is missing in a job payload.
                    payload = next(
                        (p for p in self.queues.get(q, []) if p.get("id") == jid),
                        None,  # Return None if the job payload is not found in the queue.
                    )
                    # If the job payload was found, add its details to the stalled list.
                    if payload:
                        stalled.append({"queue_name": q, "job_data": payload})

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
        # Acquire the lock to protect shared state (queues and heartbeats accessed here).
        async with self.lock:
            # Append the job data to the specified in-memory queue list.
            # setdefault ensures the queue list exists even if this is the first job
            # for this queue.
            self.queues.setdefault(queue_name, []).append(job_data)

            # Remove the heartbeat entry for this job as it's no longer running.
            # Use .pop with a default of None to avoid errors if the heartbeat was
            # already removed or if 'id' is missing.
            self.heartbeats.pop((queue_name, job_data.get("id")), None)

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
        # Acquire the lock to ensure exclusive access to in-memory queues.
        async with self.lock:
            # Get the raw count of jobs in the in-memory waiting queue.
            raw_waiting = len(self.queues.get(queue_name, []))
            # Get the count of jobs in the in-memory delayed queue.
            delayed = len(self.delayed.get(queue_name, []))

        # Query the persistent store for the count of failed jobs.
        failed_jobs = await self.store.jobs_by_status(queue_name, State.FAILED)
        failed = len(failed_jobs)

        # Calculate the effective waiting count by subtracting failed jobs (which
        # might still be in the in-memory queue list if not explicitly removed)
        # from the raw in-memory count. Ensure the result is not negative.
        waiting = raw_waiting - failed
        if waiting < 0:
            waiting = 0

        # Return a dictionary containing the calculated statistics.
        return {
            "waiting": waiting,
            "delayed": delayed,
            "failed": failed,
        }

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
        Asynchronously retrieves a list of all repeatable job definitions for a
        specific queue from the in-memory repeatable definitions dictionary.

        Iterates through the in-memory repeatable definitions for the given queue,
        deserializes the JSON job definitions, and converts them into a list
        of `RepeatableInfo` dataclass instances. The list is sorted by the
        `next_run` timestamp. Access to the in-memory repeatable definitions
        is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to list repeatable jobs for.

        Returns:
            A list of `RepeatableInfo` dataclass instances.
        """
        # Acquire the lock to ensure exclusive access to in-memory repeatable definitions.
        async with self.lock:
            out: list[RepeatableInfo] = []  # Initialize a list to store RepeatableInfo instances.
            # Iterate through the repeatable definitions for the specified queue.
            # Use .get() with default {} for safety.
            for raw, rec in self.repeatables.get(queue_name, {}).items():
                jd = json.loads(raw)  # Deserialize the JSON job definition.
                # Append a new RepeatableInfo instance to the output list.
                out.append(RepeatableInfo(job_def=jd, next_run=rec["next_run"], paused=rec["paused"]))
            # Sort the output list by the 'next_run' timestamp.
            return sorted(out, key=lambda x: x.next_run)

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously marks a specific repeatable job definition as paused in memory.

        Serializes the job definition to JSON and updates the 'paused' flag to
        True in the in-memory repeatable definitions dictionary. The scheduler
        should check this flag and skip scheduling new instances of a paused
        repeatable job. Access to the in-memory repeatable definitions is
        synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to pause.
        """
        # Acquire the lock to ensure exclusive access to in-memory repeatable definitions.
        async with self.lock:
            raw = json.dumps(job_def)  # Serialize the job definition to JSON.
            # Get the record for the repeatable job, safely handling missing queue or job.
            rec = self.repeatables.get(queue_name, {}).get(raw)
            # If the record exists.
            if rec:
                rec["paused"] = True  # Mark the repeatable job as paused.

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> Any:
        """
        Asynchronously un-pauses a repeatable job definition in memory, computes
        its next scheduled run time, and updates the definition in memory.

        Serializes the job definition to JSON, removes the old entry from the
        in-memory repeatable definitions dictionary, computes the next run time
        using `compute_next_run`, and adds the updated definition back with
        'paused' set to False. Access to the in-memory repeatable definitions
        is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to resume.

        Returns:
            The newly computed timestamp (float) for the next run of the repeatable job.

        Raises:
            KeyError: If the specified repeatable job definition is not found
                      in the in-memory definitions.
        """
        # Acquire the lock to ensure exclusive access to in-memory repeatable definitions.
        async with self.lock:
            raw = json.dumps(job_def)  # Serialize the job definition to JSON.
            # Remove the old record for the repeatable job, safely handling missing queue or job.
            rec = self.repeatables.setdefault(queue_name, {}).pop(raw, None)
            # If the record was not found, raise a KeyError.
            if rec is None:
                raise KeyError(f"Repeatable job not found: {job_def}")

            # Create a clean job definition without the 'paused' key for computing the next run.
            clean_def = {k: v for k, v in rec["job_def"].items() if k != "paused"}
            # Compute the next scheduled run timestamp using the scheduler utility function.
            next_ts = compute_next_run(clean_def)

            # Store the updated repeatable definition under the clean JSON key
            # with the new next_run timestamp and 'paused' set to False.
            self.repeatables[queue_name][json.dumps(clean_def)] = {
                "job_def": clean_def,
                "next_run": next_ts,
                "paused": False,
            }
            return next_ts  # Return the newly computed next run timestamp.

    async def cancel_job(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously cancels a job, removing it from the in-memory waiting
        and delayed queues and marking it as cancelled in memory.

        This method removes the job with the matching ID from the in-memory
        waiting queue list and the in-memory delayed queue list. It also adds
        the job ID to the in-memory set of cancelled jobs so that workers can
        check this set and skip or stop processing the job if it was in-flight.
        Access to the in-memory queues and cancelled set is synchronized using
        the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to cancel.
        """
        # Acquire the lock to ensure exclusive access to in-memory queues and cancelled set.
        async with self.lock:
            # Filter the in-memory waiting queue list, keeping jobs whose ID does not match.
            # Use .get() with default [] for safety.
            self.queues[queue_name] = [j for j in self.queues.get(queue_name, []) if j.get("id") != job_id]
            # Filter the in-memory delayed queue list, keeping jobs whose ID does not match.
            # Use .get() with default [] for safety.
            self.delayed[queue_name] = [(ts, j) for ts, j in self.delayed.get(queue_name, []) if j.get("id") != job_id]
            # Add the job ID to the in-memory set of cancelled jobs.
            # setdefault ensures the set exists even if this is the first cancelled job for this queue.
            self.cancelled.setdefault(queue_name, set()).add(job_id)

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously checks if a specific job has been marked as cancelled in memory.

        This method checks if the job ID is present in the in-memory set of
        cancelled jobs for the given queue name. Access to the in-memory cancelled
        set is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to check.

        Returns:
            True if the job ID is found in the in-memory cancelled set for the
            specified queue, False otherwise.
        """
        # Acquire the lock to ensure exclusive access to the in-memory cancelled set.
        async with self.lock:
            # Check if the job ID is present in the in-memory set of cancelled jobs
            # for the specified queue. Use .get() with default set() for safety.
            return job_id in self.cancelled.get(queue_name, set())

    async def list_jobs(self, queue: str, state: str) -> list[dict[str, Any]]:
        return await self.store.filter(queue=queue, state=state)

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
        cutoff = time.time() - monkay.settings.heartbeat_ttl
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

    async def retry_job(self, queue_name: str, job_id: str) -> None:
        """
        Retry a job by moving it from DLQ or failed state back to waiting.

        Loads the job document from storage, updates its status to WAITING,
        and persists the changes. This makes the job available for processing again.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
        """
        job = await self.store.load(queue_name, job_id)
        if job:
            from asyncmq.core.enums import State

            job["status"] = State.WAITING
            await self.store.save(queue_name, job_id, job)

    async def remove_job(self, queue_name: str, job_id: str) -> None:
        """
        Remove a job completely from storage.

        Deletes the job document with the specified ID from the given queue.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove.
        """
        await self.store.delete(queue_name, job_id)
