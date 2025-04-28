import time
from typing import Any, Tuple

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.enums import State
from asyncmq.event import event_emitter

# Define a type alias for the tuple key used in dictionaries like job_states, job_results, etc.
# It's a tuple of (queue_name: str, job_id: str). This provides a clear type hint
# for composite keys used internally.
_JobKey = Tuple[str, str]


class InMemoryBackend(BaseBackend):
    """
    An in-memory implementation of the BaseBackend for asyncmq.

    This backend stores all queue data, job states, results, dependencies,
    and delayed jobs in memory using Python dictionaries, lists, and sets.
    It is suitable for testing and simple single-process use cases but is
    not persistent (data is lost when the process stops) and does not scale
    beyond a single process. It uses an AnyIO Lock to ensure thread-safe access
    to its internal data structures when used within a single AnyIO instance.
    """
    def __init__(self) -> None:
        """
        Initializes the InMemoryBackend with empty in-memory storage structures.
        Sets up dictionaries and sets to simulate queue storage, DLQs,
        delayed jobs, job states, results, progress, and dependency tracking.
        Initializes an AnyIO Lock for thread safety.
        """
        # Dictionary mapping queue names to lists of job payloads waiting in the queue.
        # Jobs are stored as dictionaries. Order in the list represents processing order.
        self.queues: dict[str, list[dict[str, Any]]] = {}
        # Dictionary mapping queue names to lists of job payloads moved to the Dead Letter Queue.
        self.dlqs: dict[str, list[dict[str, Any]]] = {}
        # Dictionary mapping queue names to lists of tuples, where each tuple contains
        # the run_at timestamp (float) and the job payload (dict) for delayed jobs.
        self.delayed: dict[str, list[tuple[float, dict[str, Any]]]] = {}
        # Dictionary mapping (_JobKey) tuples (queue_name, job_id) to the job's status string.
        self.job_states: dict[_JobKey, str] = {}
        # Dictionary mapping (_JobKey) tuples (queue_name, job_id) to the job's result data.
        self.job_results: dict[_JobKey, Any] = {}
        # Dictionary mapping (_JobKey) tuples (queue_name, job_id) to the job's progress float.
        self.job_progress: dict[_JobKey, float] = {}
        # Dictionary mapping (_JobKey) tuples (queue_name, job_id) of child jobs
        # to a set of parent job IDs (strings) they are waiting on.
        self.deps_pending: dict[_JobKey, set[str]] = {}
        # Dictionary mapping (_JobKey) tuples (queue_name, parent_id) to a set
        # of child job IDs (strings) that depend on the parent.
        self.deps_children: dict[_JobKey, set[str]] = {}
        # A set of queue names that are currently paused.
        self.paused: set[str] = set()
        # An AnyIO Lock to protect access to the internal data structures in concurrent scenarios.
        self.lock: anyio.Lock = anyio.Lock()

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously enqueues a job payload onto the specified queue.

        The job is added to the in-memory list for the queue, and the list
        is sorted by job priority (lower number = higher priority) to maintain
        processing order. The job's initial state is set to State.WAITING.
        Access to the queue list and job_states dictionary is protected by the
        internal lock to ensure thread safety.

        Args:
            queue_name: The name of the queue.
            payload: The job data as a dictionary, expected to contain at least
                     an "id" key and optionally a "priority" key.
        """
        # Acquire the lock to safely modify shared data structures.
        async with self.lock:
            # Get the list for the queue by name, creating it if it doesn't exist.
            queue: list[dict[str, Any]] = self.queues.setdefault(queue_name, [])
            # Append the new job payload to the list.
            queue.append(payload)
            # Sort the queue list based on job priority (lower priority number is higher).
            queue.sort(key=lambda job: job.get("priority", 5))
            # Update the job's state to WAITING in the job_states dictionary.
            self.job_states[(queue_name, payload["id"])] = State.WAITING

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously attempts to dequeue the next job from the specified queue.

        Retrieves the first job from the in-memory list for the queue if the
        list is not empty. Access to the queue list is protected by the
        internal lock. The state of the dequeued job is not explicitly updated
        here; it is typically handled by the worker processing the job.

        Args:
            queue_name: The name of the queue to dequeue from.

        Returns:
            The job data as a dictionary (the first item in the queue) if the
            queue is not empty, otherwise None.
        """
        # Acquire the lock to safely access and modify the queue list.
        async with self.lock:
            # Get the list for the queue by name, defaulting to an empty list.
            queue: list[dict[str, Any]] = self.queues.get(queue_name, [])
            # If the queue list is not empty.
            if queue:
                # Remove and return the first job from the list (highest priority).
                return queue.pop(0)
            # If the queue list is empty.
            return None

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously moves a job payload to the Dead Letter Queue (DLQ)
        associated with the specified queue.

        The job is appended to the in-memory list representing the DLQ for
        that queue, and its state is updated to State.FAILED in the job_states
        dictionary. Access to the DLQ list and job_states dictionary is protected
        by the internal lock.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: The job data as a dictionary, expected to contain at least
                     an "id" key.
        """
        # Acquire the lock to safely modify shared data structures.
        async with self.lock:
            # Get the list for the DLQ by queue name, creating it if it doesn't exist.
            dlq: list[dict[str, Any]] = self.dlqs.setdefault(queue_name, [])
            # Append the job payload to the DLQ list.
            dlq.append(payload)
            # Update the job's state to FAILED in the job_states dictionary.
            self.job_states[(queue_name, payload["id"])] = State.FAILED

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        In this in-memory backend, the job is already removed from the queue
        list upon dequeueing by the `dequeue` method, and its state is managed
        separately in the `job_states` dictionary. Therefore, this method
        is a no-operation (`pass`). State updates (e.g., to COMPLETED) are
        expected to be handled by `update_job_state`.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job being acknowledged.
        """
        # This method is a no-operation in this in-memory backend.
        pass

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Asynchronously schedules a job to be available for processing at a
        specific future time by adding it to the in-memory delayed list.

        The job is stored as a tuple containing the `run_at` timestamp and the
        job payload. Its state is updated to State.EXPIRED in the job_states
        dictionary. Access to the delayed list and job_states dictionary is
        protected by the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: The job data as a dictionary, expected to contain at least
                     an "id" key.
            run_at: The absolute timestamp (e.g., from time.time()) when the
                    job should become available for processing.
        """
        # Acquire the lock to safely modify shared data structures.
        async with self.lock:
            # Get the list for delayed jobs by queue name, creating it if it doesn't exist.
            delayed_list: list[tuple[float, dict[str, Any]]] = self.delayed.setdefault(queue_name, [])
            # Append the tuple containing the run_at time and the job payload.
            delayed_list.append((run_at, payload))
            # Update the job's state to EXPIRED in the job_states dictionary.
            self.job_states[(queue_name, payload["id"])] = State.EXPIRED

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves delayed job payloads from the specified queue
        that are now due for processing (i.e., their `run_at` timestamp is
        less than or equal to the current time).

        Due jobs are returned, and the remaining delayed jobs are updated in
        the in-memory list by creating a new list excluding the due jobs.
        Access to the delayed list is protected by the internal lock.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of dictionaries, where each dictionary is a job payload
            that is ready to be moved to the main queue.
        """
        # Acquire the lock to safely access and modify the delayed list.
        async with self.lock:
            # Get the current timestamp for comparison.
            now: float = time.time()
            due_jobs: list[dict[str, Any]] = []
            still_delayed: list[tuple[float, dict[str, Any]]] = []
            # Iterate through the current delayed jobs for the queue.
            for run_at, payload in self.delayed.get(queue_name, []):
                # Check if the job's run_at timestamp is less than or equal to the current time.
                if run_at <= now:
                    # If due, add the job payload to the list of due jobs.
                    due_jobs.append(payload)
                else:
                    # If not due, keep the tuple in the list of still delayed jobs.
                    still_delayed.append((run_at, payload))
            # Replace the old delayed list with the list of jobs that are still delayed.
            self.delayed[queue_name] = still_delayed
            # Return the list of job payloads that were found to be due.
            return due_jobs

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously removes a job from the backend's delayed storage by its ID.

        In this specific in-memory backend implementation, the removal of due
        delayed jobs is handled implicitly by the `get_due_delayed` method when
        it rebuilds the delayed list. This method is therefore a no-operation
        (`pass`).

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove from delayed storage.
        """
        # This method is a no-operation in this in-memory backend.
        pass

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the status of a specific job in the in-memory
        `job_states` dictionary.

        Access to the `job_states` dictionary is protected by the internal lock
        to ensure thread safety.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            state: The new state string for the job (e.g., State.ACTIVE, State.COMPLETED).
        """
        # Acquire the lock to safely modify the shared job_states dictionary.
        async with self.lock:
            # Update the state for the job using the composite tuple key (queue_name, job_id).
            self.job_states[(queue_name, job_id)] = state

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a job's execution in the in-memory
        `job_results` dictionary.

        Access to the `job_results` dictionary is protected by the internal lock
        to ensure thread safety.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            result: The result returned by the job's task function. Can be of Any type.
        """
        # Acquire the lock to safely modify the shared job_results dictionary.
        async with self.lock:
            # Store the job result using the composite tuple key (queue_name, job_id).
            self.job_results[(queue_name, job_id)] = result

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Asynchronously retrieves the current status string of a specific job
         from the in-memory `job_states` dictionary.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The job's status string if found in the `job_states` dictionary,
            otherwise None.
        """
        # Access the job_states dictionary using the composite tuple key.
        # This is a read operation and does not require the lock.
        return self.job_states.get((queue_name, job_id))

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Asynchronously retrieves the execution result of a specific job from
        the in-memory `job_results` dictionary.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The result of the job's task function if it was previously saved
            in the `job_results` dictionary, otherwise None.
        """
        # Access the job_results dictionary using the composite tuple key.
        # This is a read operation and does not require the lock.
        return self.job_results.get((queue_name, job_id))

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Asynchronously registers a job's dependencies and the relationship
        between parent and child jobs in the in-memory dependency tracking
        dictionaries (`deps_pending` and `deps_children`).

        Updates `deps_pending` for the child job to record which parent jobs
        it is waiting on. Updates `deps_children` for each parent job to
        record which child jobs are waiting on it.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The job data dictionary, expected to contain at least
                      an "id" key and an optional "depends_on" list of parent job IDs.
        """
        # Get the ID of the child job that has dependencies.
        child_job_id: str = job_dict['id']
        # Create the composite key for the child job in dependency tracking dictionaries.
        child_pend_key: _JobKey = (queue_name, child_job_id)
        # Get the set of parent job IDs this child job depends on.
        parent_deps: set[str] = set(job_dict.get('depends_on', []))

        # If the job has no dependencies listed, there's nothing to register.
        if not parent_deps:
            return

        # Store the set of parent job IDs this child job is pending on.
        self.deps_pending[child_pend_key] = parent_deps

        # For each parent job ID in the dependencies list.
        for parent_id in parent_deps:
            # Create the composite key for the parent job in the children tracking dictionary.
            parent_children_key: _JobKey = (queue_name, parent_id)
            # Get the set of children for this parent, creating it if necessary, and add the child job ID.
            self.deps_children.setdefault(parent_children_key, set()).add(child_job_id)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and checks if
        any dependent child jobs are now ready to be enqueued.

        Removes the `parent_id` from the `deps_pending` set of all its children.
        If a child job's `deps_pending` set becomes empty, it means all its
        dependencies are met. The child job data is then (attempted to be)
        fetched, the job is enqueued into the main queue, a `job:ready` event
        is emitted, and the child's dependency tracking entry is cleaned up.
        Finally, the parent job's entry in `deps_children` is removed.

        Args:
            queue_name: The name of the queue the parent job belonged to.
            parent_id: The unique identifier of the job that just completed.
        """
        # Create the composite key for the parent job in the children tracking dictionary.
        parent_children_key: _JobKey = (queue_name, parent_id)
        # Get the set of child job IDs that depend on this parent job.
        # Get a list copy to iterate safely while modifying the original set.
        children: list[str] = list(self.deps_children.get(parent_children_key, set()))

        # Iterate through each child job ID that was waiting on the parent.
        for child_id in children:
            # Create the composite key for the child job in the pending dependencies dictionary.
            child_pend_key: _JobKey = (queue_name, child_id)
            # Check if the child job is still in the pending dependencies dictionary.
            if child_pend_key in self.deps_pending:
                # Remove the completed parent's ID from the child's pending dependencies set.
                # discard() is used as it doesn't raise an error if the element isn't found.
                self.deps_pending[child_pend_key].discard(parent_id)

                # Check if the child job now has no remaining pending dependencies.
                if not self.deps_pending[child_pend_key]:
                    # If all dependencies are met, attempt to fetch the child job data.
                    # NOTE: This fetches from the main queue list (`self.queues`),
                    # which might not be where the job payload is actually stored
                    # if it was delayed or moved to DLQ earlier. A robust backend
                    # would load from persistent storage here.
                    raw: dict[str, Any] | None = self._fetch_job_data(queue_name, child_id)
                    # If the job data was successfully found.
                    if raw:
                        # Enqueue the child job into the main queue for processing.
                        await self.enqueue(queue_name, raw)
                        # Emit a local event indicating the job is now ready.
                        await event_emitter.emit('job:ready', {'id': child_id})
                    # Remove the child job's entry from the pending dependencies dictionary.
                    del self.deps_pending[child_pend_key]

        # Remove the parent job's entry from the deps_children dictionary.
        # pop() with None default is used to avoid KeyError if the key doesn't exist.
        self.deps_children.pop(parent_children_key, None)

    async def pause_queue(self, queue_name: str) -> None:
        """
        Marks the specified queue as paused by adding its name to the in-memory set
        of paused queues.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Add the queue name to the set of paused queues.
        self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Removes the specified queue from the in-memory set of paused queues,
        allowing workers to resume processing jobs from it.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Remove the queue name from the set of paused queues.
        # discard() is used as it doesn't raise an error if the element isn't found.
        self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Checks if the specified queue is currently marked as paused in the
        in-memory set.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue name is present in the set of paused queues,
            False otherwise.
        """
        # Check if the queue name exists in the set of paused queues.
        return queue_name in self.paused

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a specific job in the
        in-memory `job_progress` dictionary.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The progress value, typically a float between 0.0 and 1.0.
        """
        # Store the job progress using the composite tuple key (queue_name, job_id).
        self.job_progress[(queue_name, job_id)] = progress

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue
        in a single batch operation by extending the in-memory queue list.

        Each job's state is set to State.WAITING, and the queue list is re-sorted
        by priority after adding the jobs. Access to the queue list and
        job_states dictionary is protected by the internal lock.

        Args:
            queue_name: The name of the queue to enqueue jobs onto.
            jobs: A list of job payloads (dictionaries) to be enqueued. Each
                  dictionary is expected to contain at least an "id" key.
        """
        # Acquire the lock to safely modify shared data structures.
        async with self.lock:
            # Get the list for the queue by name, creating it if it doesn't exist.
            q: list[dict[str, Any]] = self.queues.setdefault(queue_name, [])
            # Extend the queue list with all the jobs from the input list.
            q.extend(jobs)
            # Update the state for each newly enqueued job to WAITING.
            for job in jobs:
                self.job_states[(queue_name, job['id'])] = State.WAITING
            # Re-sort the entire queue list by job priority to maintain order after bulk addition.
            q.sort(key=lambda job: job.get("priority", 5))

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Removes jobs from the in-memory storage based on their state and
        optional age criteria.

        Iterates through all job states in `job_states` and removes matching
        entries (by queue name and state) from `job_states`, `job_results`,
        and `job_progress`.
        NOTE: This specific implementation does **not** check the `older_than`
        timestamp and purges all jobs in the specified state regardless of age.
        It also does **not** remove the job payloads from the `self.queues`,
        `self.dlqs`, or `self.delayed` lists, which means job payloads might
        remain in those lists even if their state, result, and progress are purged.

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state of the jobs to be removed (e.g., State.COMPLETED, State.FAILED).
            older_than: An optional timestamp (ignored in this specific implementation).
                        Defaults to None.
        """
        # Create a list to collect the composite keys of jobs to delete.
        to_delete: list[_JobKey] = []
        # Iterate through a copy of the job_states items to safely modify the dictionary during iteration.
        for (qname, jid), st in list(self.job_states.items()):
            # Check if the job belongs to the target queue and has the specified state.
            if qname == queue_name and st == state:
                # Append the composite key to the list of keys to delete.
                to_delete.append((qname, jid))

        # Iterate through the collected keys and remove corresponding entries
        # from the relevant dictionaries.
        for key in to_delete:
            # Remove the state entry.
            self.job_states.pop(key, None)
            # Remove the result entry.
            self.job_results.pop(key, None)
            # Remove the progress entry.
            self.job_progress.pop(key, None)
            # NOTE: Job payloads might still exist in self.queues, self.dlqs, self.delayed.

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Emits an event locally using the global event emitter.

        This in-memory backend does not implement distributed event broadcasting
        across multiple processes.

        Args:
            event: The name of the event to emit.
            data: The data associated with the event.
        """
        # Emit the event using the global event_emitter instance.
        await event_emitter.emit(event, data)

    async def create_lock(self, key: str, ttl: int) -> anyio.Lock:
        """
        Creates and returns a backend-agnostic AnyIO Lock instance.

        This in-memory implementation provides a simple local lock that is
        effective only within a single process or AnyIO instance. It does
        not implement distributed locking across multiple processes, and the
        TTL parameter is not enforced for this local lock.

        Args:
            key: A unique string identifier for the lock (used conceptually for
                 naming but the returned lock is not keyed).
            ttl: The time-to-live for the lock in seconds (ignored by this
                 local lock implementation).

        Returns:
            A new `anyio.Lock` instance.
        """
        # Return a standard AnyIO Lock instance.
        # This lock is not distributed and does not respect the key or TTL.
        return anyio.Lock()

    def _fetch_job_data(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        """
        Internal helper to find a job payload by ID within the in-memory queue list.

        This helper specifically searches the main `self.queues` list for the
        given queue name. It does **not** search `self.dlqs` or `self.delayed`.
        This might be a limitation depending on where job payloads are expected
        to reside when this helper is called (e.g., during dependency resolution).

        Args:
            queue_name: The name of the queue to search within.
            job_id: The unique identifier of the job to find.

        Returns:
            The job payload dictionary if found in the main queue list, otherwise None.
        """
        # Iterate through job dictionaries in the main queue list for the specified queue.
        for job in self.queues.get(queue_name, []):
            # Check if the job's 'id' key matches the target job ID.
            if job.get('id') == job_id:
                # Return the found job dictionary.
                return job
        # If the loop completes without finding the job in the main queue list, return None.
        return None
