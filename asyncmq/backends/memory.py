import time
from typing import Any, Dict, List, Optional, Set, Tuple

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.enums import State
from asyncmq.event import event_emitter

# Define a type alias for the tuple key used in dictionaries like job_states, job_results, etc.
# It's a tuple of (queue_name: str, job_id: str)
_JobKey = Tuple[str, str]


class InMemoryBackend(BaseBackend):
    """
    An in-memory implementation of the BaseBackend for asyncmq.

    This backend stores all queue data, job states, results, dependencies,
    and delayed jobs in memory using Python dictionaries, lists, and sets.
    It is suitable for testing and simple use cases but is not persistent
    and does not scale beyond a single process. It uses an AnyIO Lock
    to ensure thread-safe access to its internal data structures.
    """
    def __init__(self) -> None:
        """
        Initializes the InMemoryBackend with empty in-memory storage structures.
        """
        # Dictionary mapping queue names to lists of job payloads waiting in the queue.
        # Jobs are stored as dictionaries.
        self.queues: Dict[str, List[Dict[str, Any]]] = {}
        # Dictionary mapping queue names to lists of job payloads moved to the Dead Letter Queue.
        self.dlqs: Dict[str, List[Dict[str, Any]]] = {}
        # Dictionary mapping queue names to lists of tuples, where each tuple contains
        # the run_at timestamp (float) and the job payload (dict) for delayed jobs.
        self.delayed: Dict[str, List[Tuple[float, Dict[str, Any]]]] = {}
        # Dictionary mapping (_JobKey) tuples (queue_name, job_id) to the job's status string.
        self.job_states: Dict[_JobKey, str] = {}
        # Dictionary mapping (_JobKey) tuples (queue_name, job_id) to the job's result data.
        self.job_results: Dict[_JobKey, Any] = {}
        # Dictionary mapping (_JobKey) tuples (queue_name, job_id) to the job's progress float.
        self.job_progress: Dict[_JobKey, float] = {}
        # Dictionary mapping (_JobKey) tuples (queue_name, job_id) of child jobs
        # to a set of parent job IDs (strings) they are waiting on.
        self.deps_pending: Dict[_JobKey, Set[str]] = {}
        # Dictionary mapping (_JobKey) tuples (queue_name, parent_id) to a set
        # of child job IDs (strings) that depend on the parent.
        self.deps_children: Dict[_JobKey, Set[str]] = {}
        # A set of queue names that are currently paused.
        self.paused: Set[str] = set()
        # An AnyIO Lock to protect access to the internal data structures.
        self.lock: anyio.Lock = anyio.Lock()

    async def enqueue(self, queue_name: str, payload: Dict[str, Any]) -> None:
        """
        Asynchronously enqueues a job payload onto the specified queue.

        The job is added to the in-memory list for the queue and the list
        is sorted by job priority (lower number = higher priority). The job's
        initial state is set to State.WAITING. Access to the queue list and
        job_states dictionary is protected by the internal lock.

        Args:
            queue_name: The name of the queue.
            payload: The job data as a dictionary, expected to contain at least an "id" key.
        """
        async with self.lock:
            # Get the list for the queue, creating it if it doesn't exist.
            queue: List[Dict[str, Any]] = self.queues.setdefault(queue_name, [])
            # Append the job payload to the queue list.
            queue.append(payload)
            # Sort the queue by job priority.
            queue.sort(key=lambda job: job.get("priority", 5))
            # Update the job's state to State.WAITING.
            self.job_states[(queue_name, payload["id"])] = State.WAITING

    async def dequeue(self, queue_name: str) -> Optional[Dict[str, Any]]:
        """
        Asynchronously attempts to dequeue the next job from the specified queue.

        Retrieves the first job from the in-memory list for the queue. Access
        to the queue list is protected by the internal lock.

        Args:
            queue_name: The name of the queue to dequeue from.

        Returns:
            The job data as a dictionary if the queue is not empty, otherwise None.
        """
        async with self.lock:
            # Get the list for the queue.
            queue: List[Dict[str, Any]] = self.queues.get(queue_name, [])
            # If the queue is not empty, remove and return the first job.
            if queue:
                return queue.pop(0)
            # If the queue is empty, return None.
            return None

    async def move_to_dlq(self, queue_name: str, payload: Dict[str, Any]) -> None:
        """
        Asynchronously moves a job payload to the Dead Letter Queue (DLQ)
        associated with the specified queue.

        The job is appended to the in-memory list for the DLQ, and its state
        is updated to State.FAILED. Access to the DLQ list and job_states dictionary
        is protected by the internal lock.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: The job data as a dictionary, expected to contain at least an "id" key.
        """
        async with self.lock:
            # Get the list for the DLQ, creating it if it doesn't exist.
            dlq: List[Dict[str, Any]] = self.dlqs.setdefault(queue_name, [])
            # Append the job payload to the DLQ list.
            dlq.append(payload)
            # Update the job's state to State.FAILED.
            self.job_states[(queue_name, payload["id"])] = State.FAILED

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        In this in-memory backend, the job is already removed from the queue
        upon dequeueing, and its state is managed separately. Therefore, this
        method is a no-operation.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job being acknowledged.
        """
        # No-operation in this in-memory implementation.
        pass

    async def enqueue_delayed(self, queue_name: str, payload: Dict[str, Any], run_at: float) -> None:
        """
        Asynchronously schedules a job to be available for processing at a
        specific future time by adding it to the in-memory delayed list.

        The job is stored as a tuple containing the run_at timestamp and the
        job payload. Its state is updated to State.EXPIRED. Access to the delayed
        list and job_states dictionary is protected by the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: The job data as a dictionary, expected to contain at least an "id" key.
            run_at: The absolute timestamp (e.g., from time.time()) when the
                    job should become available for processing.
        """
        async with self.lock:
            # Get the list for delayed jobs, creating it if it doesn't exist.
            delayed_list: List[Tuple[float, Dict[str, Any]]] = self.delayed.setdefault(queue_name, [])
            # Append the run_at timestamp and job payload as a tuple.
            delayed_list.append((run_at, payload))
            # Update the job's state to State.EXPIRED.
            self.job_states[(queue_name, payload["id"])] = State.EXPIRED

    async def get_due_delayed(self, queue_name: str) -> List[Dict[str, Any]]:
        """
        Asynchronously retrieves delayed job payloads from the specified queue
        that are now due for processing (i.e., their `run_at` timestamp is
        less than or equal to the current time).

        Due jobs are returned, and the remaining delayed jobs are updated in
        the in-memory list. Access to the delayed list is protected by the
        internal lock.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of dictionaries, where each dictionary is a job payload
            that is ready to be moved to the main queue.
        """
        async with self.lock:
            now: float = time.time()
            due_jobs: List[Dict[str, Any]] = []
            still_delayed: List[Tuple[float, Dict[str, Any]]] = []
            # Iterate through the current delayed jobs for the queue.
            for run_at, payload in self.delayed.get(queue_name, []):
                # Check if the job is due.
                if run_at <= now:
                    due_jobs.append(payload)
                else:
                    # If not due, keep it in the list of still delayed jobs.
                    still_delayed.append((run_at, payload))
            # Update the in-memory delayed list with the jobs that are still delayed.
            self.delayed[queue_name] = still_delayed
            # Return the list of jobs that were due.
            return due_jobs

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously removes a job from the backend's delayed storage by its ID.

        In this in-memory backend, the removal of due delayed jobs is handled
        implicitly by `get_due_delayed`. Therefore, this method is a no-operation.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove from delayed storage.
        """
        # No-operation in this in-memory implementation as removal is handled by get_due_delayed.
        pass

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the status of a specific job in the in-memory
        job_states dictionary.

        Access to the job_states dictionary is protected by the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            state: The new state string for the job (e.g., State.ACTIVE, State.COMPLETED).
        """
        async with self.lock:
            # Update the state for the job using the tuple key.
            self.job_states[(queue_name, job_id)] = state

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a job's execution in the in-memory
        job_results dictionary.

        Access to the job_results dictionary is protected by the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            result: The result returned by the job's task function.
        """
        async with self.lock:
            # Store the job result using the tuple key.
            self.job_results[(queue_name, job_id)] = result

    async def get_job_state(self, queue_name: str, job_id: str) -> Optional[str]:
        """
        Asynchronously retrieves the current status string of a specific job
         from the in-memory job_states dictionary.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The job's status string if found, otherwise None.
        """
        # Access the job_states dictionary directly (read-only, doesn't require lock).
        return self.job_states.get((queue_name, job_id))

    async def get_job_result(self, queue_name: str, job_id: str) -> Optional[Any]:
        """
        Asynchronously retrieves the execution result of a specific job from
        the in-memory job_results dictionary.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The result of the job's task function if it was saved, otherwise None.
        """
        # Access the job_results dictionary directly (read-only, doesn't require lock).
        return self.job_results.get((queue_name, job_id))

    async def add_dependencies(self, queue_name: str, job_dict: Dict[str, Any]) -> None:
        """
        Asynchronously registers a job's dependencies and the relationship
        between parent and child jobs in the in-memory dependency tracking
        dictionaries.

        Updates `deps_pending` for the child job and `deps_children` for
        each parent job.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The job data dictionary, expected to contain at least
                      an "id" key and an optional "depends_on" list.
        """
        job_id: str = job_dict['id']
        pend_key: _JobKey = (queue_name, job_id)
        # Get the set of parent job IDs this job depends on.
        deps: Set[str] = set(job_dict.get('depends_on', []))

        # If there are no dependencies, there's nothing to register.
        if not deps:
            return

        # Store the set of pending dependencies for this child job.
        self.deps_pending[pend_key] = deps

        # For each parent job ID, register this job as a child dependency.
        for parent in deps:
            child_key: _JobKey = (queue_name, parent)
            # Get the set of children for this parent, creating it if necessary, and add the child job ID.
            self.deps_children.setdefault(child_key, set()).add(job_id)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and checks if
        any dependent child jobs are now ready to be enqueued.

        Removes the `parent_id` from the `deps_pending` set of all its children.
        If a child job's `deps_pending` set becomes empty, it means all its
        dependencies are met, and the child job is enqueued. Cleans up entries
        in `deps_pending` and `deps_children`.

        Args:
            queue_name: The name of the queue the parent job belonged to.
            parent_id: The unique identifier of the job that just completed.
        """
        child_key: _JobKey = (queue_name, parent_id)
        # Get the set of children that depend on this parent job.
        children: Set[str] = self.deps_children.get(child_key, set())

        # Iterate through a copy of the children set as it might be modified during iteration.
        for child_id in list(children):
            pend_key: _JobKey = (queue_name, child_id)
            # Discard the parent_id from the child's pending dependencies set.
            # discard() doesn't raise an error if the element is not found.
            if pend_key in self.deps_pending: # Check if the child job is still pending dependencies
                self.deps_pending[pend_key].discard(parent_id)

                # Check if the child job now has no pending dependencies.
                if not self.deps_pending[pend_key]:
                    # If dependencies are met, fetch the job data.
                    raw: Optional[Dict[str, Any]] = self._fetch_job_data(queue_name, child_id) # Note: This fetches from the *main* queue, which might be incorrect if the job wasn't there yet. A real backend would need to load from storage.
                    if raw:
                        # Enqueue the child job for processing.
                        await self.enqueue(queue_name, raw)
                        # Emit an event indicating the job is ready.
                        await event_emitter.emit('job:ready', {'id': child_id})
                    # Remove the child job from the pending dependencies dictionary.
                    del self.deps_pending[pend_key]

        # Remove the parent job's entry from the deps_children dictionary.
        self.deps_children.pop(child_key, None)

    async def pause_queue(self, queue_name: str) -> None:
        """
        Marks the specified queue as paused in the in-memory set.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Add the queue name to the set of paused queues.
        self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Removes the specified queue from the in-memory set of paused queues.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Remove the queue name from the set of paused queues.
        self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Checks if the specified queue is currently marked as paused in the
        in-memory set.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue name is in the paused set, False otherwise.
        """
        # Check if the queue name exists in the set of paused queues.
        return queue_name in self.paused

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a specific job in the
        in-memory job_progress dictionary.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The progress value, typically a float between 0.0 and 1.0.
        """
        # Store the job progress using the tuple key.
        self.job_progress[(queue_name, job_id)] = progress

    async def bulk_enqueue(self, queue_name: str, jobs: List[Dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue
        in a single batch operation within the in-memory list.

        Each job's state is set to State.WAITING. Access to the queue list and
        job_states dictionary is protected by the internal lock.

        Args:
            queue_name: The name of the queue to enqueue jobs onto.
            jobs: A list of job payloads (dictionaries) to be enqueued. Each
                  dictionary is expected to contain at least an "id" key.
        """
        async with self.lock:
            # Get the list for the queue, creating it if it doesn't exist.
            q: List[Dict[str, Any]] = self.queues.setdefault(queue_name, [])
            # Append all job payloads from the input list to the queue list.
            q.extend(jobs)
            # Update the state for each enqueued job to State.WAITING.
            for job in jobs:
                self.job_states[(queue_name, job['id'])] = State.WAITING
            # Re-sort the queue after adding jobs (needed if priority is used).
            q.sort(key=lambda job: job.get("priority", 5))

    async def purge(self, queue_name: str, state: str, older_than: Optional[float] = None) -> None:
        """
        Removes jobs from the in-memory storage based on their state and
        optional age criteria.

        Iterates through all job states and removes matching entries from
        `job_states`, `job_results`, and `job_progress`. Note: This
        implementation does not check `older_than` and purges all jobs
        in the specified state. It also doesn't remove jobs from the
        `queues`, `dlqs`, or `delayed` lists.

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state of the jobs to be removed (e.g., State.COMPLETED, State.FAILED).
            older_than: An optional timestamp (ignored in this implementation).
        """
        # List to collect keys of jobs to delete.
        to_delete: List[_JobKey] = []
        # Iterate through a copy of the job_states items to safely modify the dictionary.
        for (qname, jid), st in list(self.job_states.items()):
            # Check if the job belongs to the correct queue and has the specified state.
            if qname == queue_name and st == state:
                to_delete.append((qname, jid))

        # Iterate through the collected keys and remove corresponding entries.
        for key in to_delete:
            self.job_states.pop(key, None)
            self.job_results.pop(key, None)
            self.job_progress.pop(key, None)
            # Note: This implementation does not remove the job payload from
            # self.queues, self.dlqs, or self.delayed lists. A more complete
            # implementation would need to handle this.

    async def emit_event(self, event: str, data: Dict[str, Any]) -> None:
        """
        Emits an event locally using the global event emitter.

        This in-memory backend does not support distributed event broadcasting.

        Args:
            event: The name of the event to emit.
            data: The data associated with the event.
        """
        # Emit the event using the global event_emitter instance.
        await event_emitter.emit(event, data)

    async def create_lock(self, key: str, ttl: int) -> anyio.Lock:
        """
        Creates and returns a backend-agnostic AnyIO Lock instance.

        This in-memory implementation provides a simple local lock. It does
        not implement distributed locking across multiple processes, and the
        TTL parameter is not enforced.

        Args:
            key: A unique string identifier for the lock (used conceptually,
                 but the returned lock is not keyed).
            ttl: The time-to-live for the lock in seconds (ignored).

        Returns:
            A new `anyio.Lock` instance.
        """
        # Return a standard AnyIO Lock. This is not a distributed lock.
        return anyio.Lock()

    def _fetch_job_data(self, queue_name: str, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Internal helper to find a job payload by ID within the in-memory queue list.

        Note: This helper only searches the main `self.queues` list. It does not
        search `dlqs` or `delayed`. This might be a limitation depending on
        where jobs are expected to reside when dependencies are resolved.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the job.

        Returns:
            The job payload dictionary if found in the main queue list, otherwise None.
        """
        # Iterate through jobs in the main queue list for the specified queue.
        for job in self.queues.get(queue_name, []):
            # Check if the job's ID matches the target ID.
            if job.get('id') == job_id:
                return job
        # If the job was not found in the main queue list, return None.
        return None
