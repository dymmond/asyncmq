import json
import time
from typing import Any

import anyio

try:
    import motor # noqa
except ImportError:
    raise ImportError("Please install motor: `pip install motor`") from None

# Import necessary components from the asyncmq library.
from asyncmq.backends.base import BaseBackend, DelayedInfo, RepeatableInfo
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.schedulers import compute_next_run
from asyncmq.stores.mongodb import MongoDBStore


class MongoDBBackend(BaseBackend):
    """
    Implements the AsyncMQ backend interface using MongoDB as the persistent
    storage and managing queue state primarily in memory, synchronized with
    an `anyio.Lock`.

    This backend handles job queueing, dequeueing, delayed jobs, repeatable tasks,
    cancellation, and heartbeats. It utilizes a `MongoDBStore` instance for
    saving and retrieving job data to and from a MongoDB database, while maintaining
    in-memory representations of queue state for potentially faster access,
    protected by a local concurrency lock. The `connect` asynchronous method
    must be called after initialization to establish the database connection.
    """

    def __init__(self, mongo_url: str = "mongodb://localhost", database: str = "asyncmq") -> None:
        """
        Initializes the MongoDB backend, setting up the persistent store and
        in-memory data structures.

        An instance of `MongoDBStore` is created to interact with the MongoDB
        database. Various dictionaries and sets are initialized in memory to
        track queue contents, delayed jobs, paused queues, repeatable jobs,
        cancelled jobs, and job heartbeats. An `anyio.Lock` is created to
        synchronize access to these in-memory data structures, preventing
        race conditions in a concurrent environment.

        Args:
            mongo_url: The connection URL string for the MongoDB server.
                       Defaults to "mongodb://localhost".
            database: The name of the database within the MongoDB server to use
                      for storing AsyncMQ data. Defaults to "asyncmq".
        """
        # Initialize the MongoDB store for persistent job data.
        self.store: MongoDBStore = MongoDBStore(mongo_url, database)
        # In-memory storage for waiting queues: maps queue name to a list of job payloads.
        self.queues: dict[str, list[dict[str, Any]]] = {}
        # In-memory storage for delayed jobs: maps queue name to a list of (run_at, job_payload) tuples.
        self.delayed: dict[str, list[tuple[float, dict[str, Any]]]] = {}
        # In-memory set storing names of queues that are currently paused.
        self.paused: set[str] = set()
        # In-memory storage for repeatable job definitions: maps queue name to a dictionary.
        # The inner dictionary maps the JSON string of the job definition to a record
        # containing the job_def dictionary, its next_run timestamp, and paused status.
        self.repeatables: dict[str, dict[str, dict[str, Any]]] = {}
        # In-memory storage for cancelled job IDs: maps queue name to a set of job IDs.
        self.cancelled: dict[str, set[str]] = {}
        # A local lock to synchronize access to the in-memory data structures.
        self.lock: anyio.Lock = anyio.Lock()
        # In-memory storage for job heartbeats: maps a (queue_name, job_id) tuple to the last timestamp.
        self.heartbeats: dict[tuple[str, str], float] = {}

    async def connect(self) -> None:
        """
        Asynchronously establishes the connection to the MongoDB database.

        Calls the `connect` method of the underlying `MongoDBStore` instance,
        which handles the actual connection establishment and index creation
        in the database. This method must be awaited after initializing the backend.
        """
        # Establish the database connection via the MongoDB store.
        await self.store.connect()

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously enqueues a single job onto the specified queue.

        Adds the job payload to the in-memory queue list for the given queue name
        and persists the job data to the MongoDB store with its status set to
        `State.WAITING`. Access to the in-memory queue is protected by the internal
        lock to ensure thread-safe operations.

        Args:
            queue_name: The name of the queue to add the job to.
            payload: The job data as a dictionary. Must include a unique identifier
                     (typically the 'id' field).
        """
        # Acquire the lock to ensure exclusive access to the in-memory queues.
        async with self.lock:
            # Add the job payload to the end of the list for the specified queue in memory.
            # setdefault ensures the list exists, creating it if necessary.
            self.queues.setdefault(queue_name, []).append(payload)
            # Save the job data to the persistent MongoDB store, setting its status to WAITING.
            # {**payload, "status": State.WAITING} creates a new dictionary.
            await self.store.save(queue_name, payload["id"], {**payload, "status": State.WAITING})

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously attempts to dequeue the next available job from the
        specified queue.

        Checks the in-memory queue list for the given queue name. If the list
        is not empty, it removes and returns the first job from the list, and
        updates the job's status to `State.ACTIVE` in the persistent MongoDB store.
        Returns None if the in-memory queue list is empty. Access to the in-memory
        queue is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to dequeue a job from.

        Returns:
            The dequeued job's data as a dictionary if a job was available,
            otherwise None.
        """
        # Acquire the lock to ensure exclusive access to the in-memory queues.
        async with self.lock:
            # Get the in-memory queue list for the specified queue name, defaulting to an empty list
            # if the queue name is not found in the self.queues dictionary.
            queue = self.queues.get(queue_name, [])
            # Check if the queue list has any jobs.
            if queue:
                # Remove and return the first job from the queue list.
                job = queue.pop(0)
                # Update the job's status to ACTIVE in the job data dictionary.
                job["status"] = State.ACTIVE
                # Save the updated job data to the persistent MongoDB store.
                await self.store.save(queue_name, job["id"], job)
                return job
            # Return None if the queue list was empty.
            return None

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously marks a job as failed and effectively moves it to the
        Dead-Letter Queue (DLQ) by updating its status in the MongoDB store.

        Updates the job's status to `State.FAILED` in the persistent MongoDB
        store. This implementation does not use a separate in-memory DLQ
        structure; failed jobs are managed through their status in the store.
        The internal lock is acquired for consistency, although the primary
        action is on the persistent store.

        Args:
            queue_name: The name of the queue the job originally belonged to.
            payload: The job data as a dictionary. Must include a unique identifier
                     (typically the 'id' field).
        """
        # Acquire the lock for consistency, mainly to protect against concurrent
        # operations that might interact with the store state.
        async with self.lock:
            # Update the job's status to FAILED in the job data dictionary.
            payload["status"] = State.FAILED
            # Save the updated job data to the persistent MongoDB store.
            await self.store.save(queue_name, payload["id"], payload)

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        This method is a no-operation (no-op) in this MongoDB backend implementation.
        Job state transitions (like moving to COMPLETED or FAILED) are handled
        by the `update_job_state` method, which interacts with the persistent
        store. Explicit acknowledgment via this method is not required for
        state management in this design.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job being acknowledged.
        """
        # No action needed; state updates are handled by update_job_state.
        pass

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Asynchronously adds a job to the in-memory delayed queue and marks
        it as delayed in the persistent store.

        Adds the job payload and its scheduled run time (`run_at`) as a tuple
        to the in-memory delayed queue list for the given queue name. It also
        saves the job to the MongoDB store with its status set to `State.DELAYED`.
        Access to the in-memory delayed queue is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job will eventually be processed from.
            payload: The job data as a dictionary. Must include a unique identifier
                     (typically the 'id' field).
            run_at: A timestamp (float, typically seconds since the epoch) indicating
                    when the job should become eligible for processing.
        """
        # Acquire the lock to ensure exclusive access to the in-memory delayed queues.
        async with self.lock:
            # Add the (run_at timestamp, job payload) tuple to the delayed queue list in memory.
            # setdefault ensures the list exists.
            self.delayed.setdefault(queue_name, []).append((run_at, payload))
            # Save the job data to the persistent MongoDB store, setting its status to DELAYED.
            # {**payload, "status": State.DELAYED} creates a new dictionary.
            await self.store.save(queue_name, payload["id"], {**payload, "status": State.DELAYED})

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves delayed jobs from the specified queue that
        have passed their scheduled execution time.

        Iterates through the in-memory delayed queue list for the given queue name,
        identifies jobs whose `run_at` timestamp is less than or equal to the
        current time, removes these jobs from the in-memory list, and returns
        them as a list of job payloads. Access to the in-memory delayed queue
        is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of job dictionaries that are ready to be processed.
        """
        # Acquire the lock to ensure exclusive access to the in-memory delayed queues.
        async with self.lock:
            now = time.time()  # Get the current timestamp.
            due: list[dict[str, Any]] = []  # List to accumulate due jobs.
            remaining: list[tuple[float, dict[str, Any]]] = []  # List to keep track of jobs not yet due.
            # Iterate through the delayed jobs for the specified queue (or an empty list if the queue doesn't exist).
            for run_at, job in self.delayed.get(queue_name, []):
                # Check if the job's scheduled run time is now or in the past.
                if run_at <= now:
                    due.append(job)  # Add the job payload to the list of due jobs.
                else:
                    remaining.append((run_at, job))  # Add the job back to the list of remaining jobs.
            # Update the in-memory delayed queue for this queue name to contain only the remaining jobs.
            self.delayed[queue_name] = remaining
            # Return the list of job payloads that were due.
            return due

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously removes a job from the in-memory delayed queue by its ID.

        This method iterates through the in-memory delayed queue list for the
        given queue name and removes the tuple containing the job whose payload
        dictionary has a matching 'id'. Access to the in-memory delayed queue
        is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove from the delayed queue.
        """
        # Acquire the lock to ensure exclusive access to the in-memory delayed queues.
        async with self.lock:
            # Filter the in-memory delayed queue list for the specified queue.
            # Keep only the (timestamp, job_payload) tuples where the job_payload's
            # 'id' does *not* match the job_id to be removed.
            # Use .get() with default [] for safety if the queue doesn't exist.
            self.delayed[queue_name] = [
                (ts, job) for ts, job in self.delayed.get(queue_name, []) if job.get("id") != job_id
            ]

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the processing state of a job in the MongoDB store.

        Loads the job data from the persistent store using the job ID, updates
        its 'status' field with the new state string, and saves the modified
        job data back to the store. This method primarily interacts with the
        persistent store, but the internal lock is acquired for consistency
        with other operations that might modify the store state.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to update.
            state: A string representing the new state of the job
                   (e.g., 'active', 'completed', 'failed').
        """
        # Acquire the lock for consistency across operations.
        async with self.lock:
            # Load the job data from the persistent MongoDB store.
            job = await self.store.load(queue_name, job_id)
            # If the job data was successfully loaded.
            if job:
                job["status"] = state  # Update the 'status' field with the new state.
                # Save the updated job data back to the persistent store.
                await self.store.save(queue_name, job_id, job)

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a completed job in the MongoDB store.

        Loads the job data from the persistent store using the job ID, adds or
        updates the 'result' field with the execution result, and saves the
        modified job data back to the store. The internal lock is acquired for
        consistency.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job whose result is to be saved.
            result: The result data of the job. This should be a value that is
                    JSON serializable by the underlying job store.
        """
        # Acquire the lock for consistency across operations.
        async with self.lock:
            # Load the job data from the persistent MongoDB store.
            job = await self.store.load(queue_name, job_id)
            # If the job data was successfully loaded.
            if job:
                job["result"] = result  # Add or update the 'result' field.
                # Save the updated job data back to the persistent store.
                await self.store.save(queue_name, job_id, job)

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Asynchronously retrieves the current status string of a specific job
        from the MongoDB store.

        Loads the job data from the persistent store using the job ID and returns
        the value of the 'status' field if the job is found. Returns None if the
        job is not found or does not have a 'status' field. The internal lock is
        acquired for consistency.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job whose state is requested.

        Returns:
            The job's status string (e.g., 'waiting', 'active', 'completed')
            if the job is found and has a status, otherwise None.
        """
        # Acquire the lock for consistency across operations.
        async with self.lock:
            # Load the job data from the persistent MongoDB store.
            job = await self.store.load(queue_name, job_id)
            # Return the value of the 'status' field if the job dictionary exists, otherwise None.
            return job.get("status") if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Asynchronously retrieves the execution result of a specific job from
        the MongoDB store.

        Loads the job data from the persistent store using the job ID and returns
        the value of the 'result' field if the job is found and has a result.
        Returns None if the job is not found or does not have a 'result' field.
        The internal lock is acquired for consistency.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job whose result is requested.

        Returns:
            The job's result data if the job is found and has a result,
            otherwise None. The type of the result depends on what was saved
            by `save_job_result`.
        """
        # Acquire the lock for consistency across operations.
        async with self.lock:
            # Load the job data from the persistent MongoDB store.
            job = await self.store.load(queue_name, job_id)
            # Return the value of the 'result' field if the job dictionary exists, otherwise None.
            return job.get("result") if job else None

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Asynchronously registers dependencies for a single job in the MongoDB store.

        Loads the job from the persistent store, adds the parent job IDs specified
        in the 'depends_on' field of the input `job_dict` to the job's existing
        'depends_on' list (if any), and saves the updated job back to the store.
        The internal lock is acquired for consistency.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The dictionary representing the job, which must contain
                      an 'id' and is expected to contain a 'depends_on' key with
                      a list of parent job IDs this job depends on.
        """
        # Acquire the lock for consistency across operations.
        async with self.lock:
            # Load the job data from the persistent MongoDB store using the job ID from the input dict.
            job = await self.store.load(queue_name, job_dict["id"])
            # If the job data was successfully loaded from the store.
            if job:
                # Get the list of parent job IDs this job depends on from the input dictionary.
                pending: list[str] = job_dict.get("depends_on", [])
                # Get the existing dependencies list from the loaded job data, defaulting to an empty list.
                existing_deps: list[str] = job.get("depends_on", [])

                # Iterate through the new pending dependencies.
                for parent in pending:
                    # If the parent ID is not already present in the existing dependencies list.
                    if parent not in existing_deps:
                        # Add the parent ID to the existing dependencies list.
                        existing_deps.append(parent)

                # Update the 'depends_on' field in the job data with the combined list of dependencies.
                job["depends_on"] = existing_deps
                # Save the updated job data back to the persistent MongoDB store.
                await self.store.save(queue_name, job["id"], job)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and resolves
        dependencies for any child jobs waiting on it in the MongoDB store.

        This method is intended to find dependent child jobs in the persistent
        store that are waiting on `parent_id`, remove `parent_id` from their
        'depends_on' list, and if a child job's dependencies become empty,
        update its status to `State.WAITING` and add it to the in-memory queue.
        However, the original code provided for this method is a `pass` statement,
        indicating that the actual implementation for querying dependent jobs
        from the store and updating them is missing or not included here.
        The internal lock is acquired for consistency.

        Args:
            queue_name: The name of the queue containing the dependent jobs.
            parent_id: The unique identifier of the parent job that has completed.
        """
        # Acquire the lock for consistency across operations.
        async with self.lock:
            # Placeholder for the actual dependency resolution logic.
            # This implementation currently does nothing ('pass' statement).
            # A full implementation would query the store for jobs with
            # 'parent_id' in their 'depends_on' list, update those jobs,
            # and enqueue them if all dependencies are met.
            pass

    async def pause_queue(self, queue_name: str) -> None:
        """
        Asynchronously pauses processing for a specific queue by marking its name
        in the in-memory set of paused queues.

        Workers checking the `is_queue_paused` method are expected to cease
        dequeueing new jobs from queues whose names are present in this set.
        Access to the in-memory paused set is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Add the queue name to the set of paused queues in memory.
            self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Asynchronously resumes processing for a specific queue by removing its
        name from the in-memory set of paused queues.

        Workers checking the `is_queue_paused` method will no longer find the
        queue name in the paused set and can resume dequeueing jobs from it.
        Access to the in-memory paused set is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Remove the queue name from the set of paused queues in memory.
            # discard() is used to avoid raising an error if the queue was not in the set.
            self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Asynchronously checks if a specific queue is currently marked as paused
        in the in-memory set.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue name is found in the in-memory set of paused queues,
            False otherwise. Access to the in-memory paused set is synchronized
            using the internal lock.
        """
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Check if the queue name exists in the in-memory set of paused queues.
            # Use .get() with a default empty set for safety.
            return queue_name in self.paused.get(queue_name, set())

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a running job in the
        MongoDB store.

        Loads the job data from the persistent store, adds or updates the 'progress'
        field within its dictionary with the provided value, and saves the modified
        job data back to the store. This allows external components to monitor
        the progress of ongoing jobs. The internal lock is acquired for consistency.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: A float between 0.0 and 1.0 (inclusive) representing the
                      current progress of the job.
        """
        # Acquire the lock for consistency across operations.
        async with self.lock:
            # Load the job data from the persistent MongoDB store.
            job = await self.store.load(queue_name, job_id)
            # If the job data was successfully loaded.
            if job:
                job["progress"] = progress  # Set or update the 'progress' field.
                # Save the modified job data back to the persistent store.
                await self.store.save(queue_name, job_id, job)

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue.

        Adds the list of job payloads to the end of the in-memory queue list
        for the given queue name. Each job in the list is also individually
        saved to the MongoDB store with its status set to `State.WAITING`.
        Access to the in-memory queue is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to enqueue jobs onto.
            jobs: A list of job payloads (dictionaries) to be enqueued. Each
                  dictionary must include a unique identifier (typically the 'id' field).
        """
        # Acquire the lock to ensure exclusive access to the in-memory queues.
        async with self.lock:
            # Extend the in-memory queue list with the list of provided jobs.
            # setdefault ensures the list exists.
            self.queues.setdefault(queue_name, []).extend(jobs)
            # Iterate through each job in the list and save it to the persistent store
            # with the WAITING status.
            for job in jobs:
                await self.store.save(queue_name, job["id"], {**job, "status": State.WAITING})

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Asynchronously removes jobs from the MongoDB store based on their state
        and optionally, their age.

        Retrieves jobs matching the specified state from the persistent store.
        If `older_than` (a timestamp) is provided, it filters these jobs and
        deletes only those whose relevant timestamp (e.g., 'completed_at' for
        completed/failed jobs) is older than the specified value. If `older_than`
        is None, all jobs in the specified state are purged from the store.
        The internal lock is acquired for consistency.

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state string of the jobs to target for purging (e.g.,
                   'COMPLETED', 'FAILED').
            older_than: An optional Unix timestamp (float). If provided, only jobs
                        in the specified `state` whose relevant timestamp is
                        strictly before this timestamp will be purged. If None,
                        all jobs in the specified state are purged from the store.
                        Defaults to None.
        """
        # Acquire the lock for consistency across operations.
        async with self.lock:
            # Retrieve jobs from the persistent store that match the specified state.
            jobs = await self.store.jobs_by_status(queue_name, state)
            now = time.time()  # Get the current timestamp for age comparison.
            # Iterate through the jobs retrieved from the store.
            for job in jobs:
                # Determine the relevant timestamp for age comparison. Using 'completed_at'
                # as the primary timestamp, defaulting to the current time if not found.
                # This might need adjustment based on the specific 'state' being purged.
                ts = job.get("completed_at", now)
                # Check if 'older_than' is not provided (purge all in state)
                # or if the job's timestamp is strictly less than 'older_than'.
                if older_than is None or ts < older_than:
                    # Delete the job from the persistent store.
                    await self.store.delete(queue_name, job["id"])

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Asynchronously emits a backend-specific lifecycle event using the global
        event emitter.

        This allows the MongoDB backend to signal various events (e.g., job
        status changes, backend initialization) which can be observed by other
        parts of the application.

        Args:
            event: A string representing the name of the event to emit.
            data: A dictionary containing data associated with the event.
        """
        # Emit the event using the core event emitter utility.
        await event_emitter.emit(event, data)

    async def create_lock(self, key: str, ttl: int) -> anyio.Lock:
        """
        Creates and returns a backend-specific distributed lock instance.

        In this MongoDB backend implementation, this method returns an `anyio.Lock`.
        It's important to note that `anyio.Lock` provides synchronization for
        tasks running concurrently within the same process or thread group using
        `anyio`, but it *does not* provide a true distributed lock across multiple
        processes or machines. For distributed locking with MongoDB, a different
        implementation (e.g., using MongoDB's transactions or a dedicated lock
        collection) would be required. The `key` and `ttl` arguments are part
        of the `BaseBackend` interface but are not directly used by the `anyio.Lock`.

        Args:
            key: A unique string identifier for the lock (not used by the returned lock).
            ttl: The time-to-live (in seconds) for the lock (not used by the returned lock).

        Returns:
            An `anyio.Lock` instance for local concurrency control.
            The return type is `anyio.Lock` as per the original code.
        """
        # Return a standard anyio.Lock instance, providing local process/thread synchronization.
        return anyio.Lock()

    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> list[str]:
        """
        Atomically enqueues multiple jobs and registers their dependencies within
        a single operation protected by the internal lock.

        This method first enqueues all jobs provided in `job_dicts` into the
        in-memory queue and saves them to the persistent MongoDB store with
        `State.WAITING`. It then iterates through the `dependency_links`,
        loads each child job from the store, adds the parent ID to the child's
        'depends_on' list (if not already present), and saves the updated child
        job back to the store. The entire sequence of in-memory and store
        operations is wrapped in an `async with self.lock` block to ensure
        atomicity with respect to other operations on this backend instance.

        Args:
            queue_name: The name of the queue where all jobs in the flow will reside.
            job_dicts: A list of job payloads (dictionaries) to enqueue as part
                       of the flow. Each dictionary must include a unique identifier
                       (typically the 'id' field).
            dependency_links: A list of tuples, where each tuple is
                              (parent_job_id, child_job_id), defining the
                              dependencies between jobs within the flow.

        Returns:
            A list of job IDs (strings) corresponding to the jobs that were
            successfully enqueued as part of this atomic flow operation, in the
            order they were provided in `job_dicts`.
        """
        # Acquire the lock to ensure exclusive access to in-memory queues and store operations
        # during the entire flow addition process.
        async with self.lock:
            created_ids: list[str] = []  # List to store the IDs of enqueued jobs.
            # Enqueue payloads into the in-memory queue and save them to the persistent store.
            for payload in job_dicts:
                # Add the job payload to the in-memory queue list for this queue.
                self.queues.setdefault(queue_name, []).append(payload)
                # Save the job data to the persistent MongoDB store with the WAITING status.
                await self.store.save(queue_name, payload["id"], {**payload, "status": State.WAITING})
                # Add the job's ID to the list of created IDs.
                created_ids.append(payload["id"])

            # Register dependencies by loading child jobs, updating their 'depends_on' lists,
            # and saving them back to the store.
            for parent, child in dependency_links:
                # Load the child job data from the persistent store.
                job = await self.store.load(queue_name, child)
                # If the child job data was successfully loaded.
                if job:
                    # Get the existing dependencies list from the loaded job, defaulting to an empty list.
                    deps = job.get("depends_on", [])
                    # If the parent ID is not already in the dependencies list.
                    if parent not in deps:
                        # Add the parent ID to the dependencies list.
                        deps.append(parent)
                        # Update the 'depends_on' field in the job data dictionary.
                        job["depends_on"] = deps
                        # Save the modified child job data back to the persistent store.
                        await self.store.save(queue_name, child, job)

            # Return the list of IDs of the jobs that were enqueued.
            return created_ids

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Asynchronously records or updates the last heartbeat timestamp for a
        specific job in the in-memory heartbeats dictionary.

        This method acquires the internal lock to ensure safe concurrent access
        to the shared heartbeats dictionary. It stores the provided `timestamp`
        associated with a tuple combining the `queue_name` and `job_id`. This
        information is used by the stalled job detection mechanism to identify
        jobs that may have become unresponsive.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job sending the heartbeat.
            timestamp: The Unix timestamp (float) representing the time the
                       heartbeat was received.
        """
        # Acquire the lock to protect the shared in-memory heartbeats dictionary.
        async with self.lock:
            # Store the timestamp using a tuple of queue name and job ID as the key.
            self.heartbeats[(queue_name, job_id)] = timestamp

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves jobs whose last recorded heartbeat in memory is
        older than a specified timestamp, potentially indicating a stalled worker.

        This method iterates through the in-memory `heartbeats` dictionary. If
        a job's heartbeat timestamp is strictly less than the `older_than` timestamp,
        it's considered stalled. The method then attempts to find the full job
        payload in the corresponding in-memory queue (`self.queues`). If the
        payload is found, a dictionary containing the 'queue_name' and 'job_data'
        is added to the result list. Access to both `heartbeats` and `queues` is
        synchronized using the internal lock.

        Args:
            older_than: A Unix timestamp (float). Jobs with a heartbeat timestamp
                        strictly less than this value will be identified as stalled.

        Returns:
            A list of dictionaries. Each dictionary contains 'queue_name' and
            'job_data' (the full job payload dictionary) for each stalled job found
            that also exists in the in-memory queue.
        """
        stalled: list[dict[str, Any]] = []  # Initialize a list to collect stalled job details.

        # Acquire the lock to protect the shared in-memory heartbeats and queues.
        async with self.lock:
            # Iterate over a copy of the heartbeats dictionary items. Copying prevents issues
            # if the dictionary is modified concurrently (though the lock should prevent this).
            # Using list() creates the copy.
            for (q, jid), ts in list(self.heartbeats.items()):
                # Check if the heartbeat timestamp is older than the provided threshold.
                if ts < older_than:
                    # The job is potentially stalled. Try to find its full payload in the in-memory queue.
                    # This generator expression searches the list for the job with the matching ID.
                    # next() with a default of None returns the first match or None if not found.
                    payload = next(
                        (p for p in self.queues.get(q, []) if p.get("id") == jid),
                        None,  # Default value if the job is not found in the queue list.
                    )
                    # If the job payload was found in the in-memory queue.
                    if payload:
                        # Add a dictionary containing the queue name and job data to the stalled list.
                        stalled.append({"queue_name": q, "job_data": payload})

        # Return the list of found stalled jobs.
        return stalled

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        """
        Asynchronously re-enqueues a job that was previously identified as stalled
        back onto its waiting queue in memory and removes its heartbeat record.

        This method appends the provided `job_data` dictionary to the end of the
        in-memory queue list for the specified `queue_name`. It then removes the
        corresponding heartbeat entry for this job from the in-memory `heartbeats`
        dictionary. Access to both the in-memory queue and heartbeats is
        synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job should be re-enqueued into.
            job_data: The dictionary containing the job's data, which is appended
                      to the in-memory queue list. This dictionary is expected to
                      contain a unique identifier (typically the 'id' field) for
                      heartbeat removal.
        """
        # Acquire the lock to protect the shared in-memory queues and heartbeats.
        async with self.lock:
            # Append the job data to the in-memory queue list for the specified queue.
            # setdefault ensures the list exists, creating it if necessary.
            self.queues.setdefault(queue_name, []).append(job_data)

            # Remove the heartbeat entry for this job from the heartbeats dictionary.
            # Use .pop() with a default of None to prevent a KeyError if the entry
            # was already removed or if 'id' is missing in job_data.
            self.heartbeats.pop((queue_name, job_data.get("id")), None)

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Asynchronously provides statistics about the number of jobs currently in
        the waiting, delayed, and failed (DLQ) states for the given queue.

        Retrieves the counts of jobs from the in-memory waiting and delayed queues.
        Queries the persistent MongoDB store for the count of failed jobs.
        Calculates the effective waiting count by subtracting the count of failed
        jobs (which might still reside in the in-memory waiting queue list if
        not explicitly removed) from the raw in-memory waiting count, ensuring
        the result is not negative. Access to the in-memory queues is synchronized
        using the internal lock.

        Args:
            queue_name: The name of the queue to retrieve statistics for.

        Returns:
            A dictionary containing the counts for "waiting", "delayed", and
            "failed" jobs. Counts are defaulted to 0 if no jobs are found in
            a specific state or queue.
        """
        # Acquire the lock to ensure exclusive access to the in-memory queues.
        async with self.lock:
            # Get the raw count of jobs in the in-memory waiting queue list.
            # Use .get() with default [] for safety.
            raw_waiting = len(self.queues.get(queue_name, []))
            # Get the count of jobs in the in-memory delayed queue list.
            # Use .get() with default [] for safety.
            delayed = len(self.delayed.get(queue_name, []))

        # Query the persistent MongoDB store to get the count of failed jobs for the queue.
        failed_jobs = await self.store.jobs_by_status(queue_name, State.FAILED)
        failed = len(failed_jobs)

        # Calculate the effective waiting count. Subtract failed jobs from the raw
        # in-memory count, as failed jobs might still be in the in-memory list.
        # Ensure the result is not negative using max().
        waiting = max(0, raw_waiting - failed)

        # Return a dictionary containing the calculated statistics for each state.
        return {
            "waiting": waiting,
            "delayed": delayed,
            "failed": failed,
        }

    async def list_delayed(self, queue_name: str) -> list[DelayedInfo]:
        """
        Asynchronously retrieves a list of all currently delayed jobs for a
        specific queue from the in-memory delayed queue.

        Iterates through the in-memory delayed queue list for the given queue name,
        sorts the jobs by their scheduled run time (`run_at`), and converts them
        into a list of `DelayedInfo` dataclass instances, including the job ID,
        scheduled run time, and the full job payload. Access to the in-memory
        delayed queue is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to list delayed jobs for.

        Returns:
            A list of `DelayedInfo` dataclass instances, sorted ascending by their
            `run_at` timestamp.
        """
        # Acquire the lock to ensure exclusive access to the in-memory delayed queue.
        async with self.lock:
            out: list[DelayedInfo] = []  # Initialize an empty list to store the results.
            # Get the delayed jobs list for the specified queue, defaulting to an empty list.
            # Sort the list by the run_at timestamp (the first element of the tuple).
            for run_at, job in sorted(self.delayed.get(queue_name, []), key=lambda x: x[0]):
                # Append a new DelayedInfo instance to the output list.
                out.append(DelayedInfo(job_id=job["id"], run_at=run_at, payload=job))
            # Return the sorted list of DelayedInfo instances.
            return out

    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Asynchronously retrieves a list of all repeatable job definitions for a
        specific queue from the in-memory repeatable definitions dictionary.

        Iterates through the in-memory repeatable definitions stored under the
        given queue name. For each definition, it deserializes the JSON key,
        retrieves the stored record (containing job_def, next_run, and paused status),
        and converts this information into a `RepeatableInfo` dataclass instance.
        The resulting list is sorted by the `next_run` timestamp. Access to the
        in-memory repeatable definitions is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to list repeatable jobs for.

        Returns:
            A list of `RepeatableInfo` dataclass instances, sorted ascending by
            their `next_run` timestamp.
        """
        # Acquire the lock to ensure exclusive access to the in-memory repeatable definitions.
        async with self.lock:
            out: list[RepeatableInfo] = []  # Initialize an empty list to store the results.
            # Get the repeatable definitions dictionary for the specified queue, defaulting to an empty dictionary.
            # Iterate through the items (raw JSON key, record) in this dictionary.
            for raw, rec in self.repeatables.get(queue_name, {}).items():
                # Deserialize the raw JSON string key back into a job definition dictionary.
                jd = json.loads(raw)
                # Append a new RepeatableInfo instance to the output list.
                # The job_def, next_run, and paused status are taken from the stored record.
                out.append(RepeatableInfo(job_def=jd, next_run=rec["next_run"], paused=rec["paused"]))
            # Sort the output list of RepeatableInfo instances by their 'next_run' attribute.
            return sorted(out, key=lambda x: x.next_run)

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously marks a specific repeatable job definition as paused in
        the in-memory repeatable definitions dictionary.

        Serializes the provided job definition dictionary to a JSON string to use
        as the key in the in-memory dictionary. It then locates the corresponding
        record for this repeatable job under the specified queue name and updates
        its 'paused' flag to True. The scheduler should check this flag and skip
        creating new instances of a paused repeatable job. Access to the in-memory
        repeatable definitions is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to pause.
        """
        # Acquire the lock to ensure exclusive access to the in-memory repeatable definitions.
        async with self.lock:
            # Serialize the job definition dictionary to a JSON string, which is used as the key.
            raw = json.dumps(job_def)
            # Get the inner dictionary of repeatable definitions for the specified queue.
            # Use .get() with an empty dictionary default for safety if the queue doesn't exist.
            queue_repeatables = self.repeatables.get(queue_name, {})
            # Get the specific record for the repeatable job using the JSON string key.
            rec = queue_repeatables.get(raw)
            # If the record for the repeatable job exists in memory.
            if rec:
                rec["paused"] = True  # Update the 'paused' flag to True.

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Asynchronously un-pauses a repeatable job definition in memory, computes
        its next scheduled run time, and updates the definition in memory.

        Serializes the provided job definition dictionary to a JSON string.
        It removes the old record (potentially marked as paused) from the
        in-memory repeatable definitions dictionary. It then computes the next
        scheduled run time for the 'cleaned' job definition (without the 'paused'
        key) using `compute_next_run`, and adds a new record back into the
        in-memory dictionary with 'paused' set to False and the new `next_run`
        timestamp. Access to the in-memory repeatable definitions is synchronized
        using the internal lock.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to resume. This
                     dictionary should contain the necessary information for
                     `compute_next_run` (e.g., schedule details).

        Returns:
            The newly computed timestamp (float) for the next run of the repeatable job.

        Raises:
            KeyError: If the specified repeatable job definition is not found
                      in the in-memory definitions for the given queue.
        """
        # Acquire the lock to ensure exclusive access to the in-memory repeatable definitions.
        async with self.lock:
            # Serialize the original job definition to JSON to use as the key for removal.
            raw = json.dumps(job_def)
            # Remove the record for the repeatable job from the in-memory dictionary.
            # Use .pop() to remove the item and return its value, or None if not found.
            # setdefault ensures the inner dictionary exists before attempting pop.
            rec = self.repeatables.setdefault(queue_name, {}).pop(raw, None)
            # If the record was not found (pop returned None), raise a KeyError.
            if rec is None:
                raise KeyError(f"Repeatable job not found for queue '{queue_name}': {job_def}")

            # Create a new job definition dictionary by copying the original job_def
            # from the record and removing the 'paused' key if present.
            clean_def = {k: v for k, v in rec["job_def"].items() if k != "paused"}
            # Compute the next scheduled run timestamp using the scheduler utility function.
            next_ts = compute_next_run(clean_def)

            # Serialize the 'cleaned' job definition back to JSON, which will be the new key.
            clean_raw = json.dumps(clean_def)
            # Add a new record for the repeatable job using the cleaned JSON key.
            # Store the cleaned job definition, the new next_run timestamp, and 'paused' set to False.
            self.repeatables[queue_name][clean_raw] = {
                "job_def": clean_def,
                "next_run": next_ts,
                "paused": False,
            }
            # Return the newly computed next run timestamp.
            return next_ts

    async def cancel_job(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously cancels a job by removing it from the in-memory waiting
        and delayed queues and marking its ID in the in-memory cancelled set.

        This method removes the job with the matching `job_id` from the in-memory
        waiting queue list (`self.queues`) and the in-memory delayed queue list
        (`self.delayed`). It then adds the `job_id` to the in-memory set of
        cancelled jobs (`self.cancelled`) for the given queue. Workers checking
        `is_job_cancelled` are expected to stop or skip processing jobs found
        in this set. Access to these in-memory structures is synchronized using
        the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to cancel.
        """
        # Acquire the lock to ensure exclusive access to the in-memory queues and cancelled set.
        async with self.lock:
            # Filter the in-memory waiting queue list for the specified queue, keeping jobs
            # whose payload 'id' does not match the job_id to be cancelled.
            # Use .get() with default [] for safety.
            self.queues[queue_name] = [
                j for j in self.queues.get(queue_name, []) if j.get("id") != job_id
            ]
            # Filter the in-memory delayed queue list for the specified queue, keeping tuples
            # where the job payload 'id' does not match the job_id to be cancelled.
            # Use .get() with default [] for safety.
            self.delayed[queue_name] = [
                (ts, j) for ts, j in self.delayed.get(queue_name, []) if j.get("id") != job_id
            ]
            # Add the job ID to the in-memory set of cancelled jobs for this queue.
            # setdefault ensures the set exists, creating it if necessary.
            self.cancelled.setdefault(queue_name, set()).add(job_id)

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously checks if a specific job has been marked as cancelled
        in the in-memory cancelled set.

        This method checks for the presence of the `job_id` within the in-memory
        set of cancelled jobs for the given `queue_name`. Workers typically use
        this method to determine if they should skip processing a job. Access
        to the in-memory cancelled set is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to check.

        Returns:
            True if the `job_id` is found in the in-memory cancelled set for the
            specified queue, indicating it has been cancelled; False otherwise.
        """
        # Acquire the lock to ensure exclusive access to the in-memory cancelled set.
        async with self.lock:
            # Check if the job_id is a member of the cancelled set for the specified queue.
            # Use .get() with a default empty set for safety if the queue has no cancelled jobs yet.
            return job_id in self.cancelled.get(queue_name, set())

    async def list_jobs(self, queue: str, state: str) -> list[dict[str, Any]]:
        """
        Asynchronously lists jobs in a specific queue filtered by their state
        from the persistent MongoDB store.

        This method delegates the job filtering by queue and state to the
        underlying `MongoDBStore` instance.

        Args:
            queue: The name of the queue to list jobs from.
            state: The state string to filter jobs by (e.g., "waiting", "active",
                   "completed", "failed", "delayed").

        Returns:
            A list of job payload dictionaries for jobs matching the criteria
            found in the MongoDB store.
        """
        # Delegate the filtering and listing of jobs to the persistent MongoDB store.
        return await self.store.filter(queue=queue, state=state)
