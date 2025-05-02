import json
import time
from typing import Any

import anyio

from asyncmq.backends.base import BaseBackend, DelayedInfo, RepeatableInfo
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.schedulers import compute_next_run

# Type alias for a job key, which is a tuple of (queue_name, job_id).
_JobKey = tuple[str, str]


class InMemoryBackend(BaseBackend):
    """
    An in-memory implementation of the AsyncMQ backend interface.

    This backend stores all job data, queue state, results, progress, and
    dependencies directly in volatile Python dictionaries, lists, and sets
    in memory. It is primarily suitable for testing, development, and light
    use cases where data persistence across application restarts is not
    required. Thread-safe access to the internal data structures within a
    single process is managed using an `anyio.Lock`.
    """

    def __init__(self) -> None:
        """
        Initializes the in-memory backend with empty data structures to hold
        all job-related information.

        Sets up dictionaries for managing waiting queues, dead letter queues,
        delayed jobs, repeatable job definitions, job states, results, progress,
        dependency tracking, active jobs, and heartbeats. Initializes a set
        for tracking paused queues and an `anyio.Lock` for concurrent access
        synchronization.
        """
        # Waiting queues: maps queue name (str) to a list of job payloads (dict).
        self.queues: dict[str, list[dict[str, Any]]] = {}
        # Dead-letter queues (DLQs): maps queue name (str) to a list of failed job payloads (dict).
        self.dlqs: dict[str, list[dict[str, Any]]] = {}
        # Delayed jobs: maps queue name (str) to a list of (run_at_timestamp, job_payload) tuples.
        self.delayed: dict[str, list[tuple[float, dict[str, Any]]]] = {}
        # Repeatable job definitions: maps queue name (str) to a nested dictionary.
        # The inner dict maps the JSON string representation of a job definition
        # to a dictionary containing the job definition, its next run timestamp,
        # and a boolean indicating if it's paused.
        self.repeatables: dict[str, dict[str, dict[str, Any]]] = {}
        # Cancelled jobs: maps queue name (str) to a set of job IDs (str) that have been cancelled.
        self.cancelled: dict[str, set[str]] = set()

        # Job state tracking: maps a job key (_JobKey) to its status string (str).
        self.job_states: dict[_JobKey, str] = {}
        # Job result tracking: maps a job key (_JobKey) to the job's execution result (Any).
        self.job_results: dict[_JobKey, Any] = {}
        # Job progress tracking: maps a job key (_JobKey) to the job's progress percentage (float).
        self.job_progress: dict[_JobKey, float] = {}

        # Dependency tracking for pending parent jobs: maps a child job key (_JobKey)
        # to a set of parent job IDs (str) it is waiting on.
        self.deps_pending: dict[_JobKey, set[str]] = {}
        # Dependency tracking for dependent children: maps a parent job key (_JobKey)
        # to a set of child job IDs (str) that are waiting on this parent.
        self.deps_children: dict[_JobKey, set[str]] = {}

        # Paused queues: a set of queue names (str) that are currently paused.
        self.paused: set[str] = set()

        # Job heartbeats: maps a job key (_JobKey) to the timestamp (float) of its last heartbeat.
        self.heartbeats: dict[_JobKey, float] = {}
        # Active jobs: maps a job key (_JobKey) to the job's payload dictionary (dict).
        # This is used internally, e.g., for stalled job detection.
        self.active_jobs: dict[_JobKey, dict[str, Any]] = {}

        # Anyio Lock for synchronizing access to all internal in-memory data structures.
        self.lock = anyio.Lock()

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously enqueues a job payload onto the specified queue.

        Adds the job payload dictionary to the end of the in-memory list for the
        given queue name. Updates the job's state to `State.WAITING` in the
        `job_states` dictionary. The queue list is then sorted by job priority
        (lower `priority` value means higher priority) to ensure jobs are
        dequeued in the correct order. Access to in-memory structures is
        synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to add the job to.
            payload: A dictionary containing the job data. Must include a unique
                     identifier (typically the 'id' field). Can optionally include
                     a 'priority' key (int), defaulting to 5 if not present.
        """
        # Acquire the lock to ensure exclusive access to shared in-memory data.
        async with self.lock:
            # Get the list for the specified queue name from the self.queues dictionary.
            # Use setdefault to create an empty list for the queue name if it doesn't exist yet.
            queue = self.queues.setdefault(queue_name, [])
            # Append the new job payload dictionary to the end of the queue list.
            queue.append(payload)
            # Sort the queue list based on the 'priority' key of each job dictionary.
            # Jobs with lower priority numbers will appear earlier in the list (higher priority).
            # Defaults to priority 5 if the key is missing.
            queue.sort(key=lambda job: job.get("priority", 5))
            # Update the job's state to WAITING in the job_states dictionary.
            self.job_states[(queue_name, payload["id"])] = State.WAITING

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously attempts to dequeue the next job from the specified queue.

        Removes and returns the first job (highest priority) from the in-memory
        queue list for the given queue name if the list is not empty. This method
        only removes the job from the queue list; the update of the job's state
        to `State.ACTIVE` is handled by the worker process after successfully
        dequeuing the job. Returns None if the in-memory queue list is empty.
        Access to the in-memory queue is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to dequeue a job from.

        Returns:
            The job data as a dictionary if a job was available in the queue,
            otherwise None.
        """
        # Acquire the lock to ensure exclusive access to the in-memory queues.
        async with self.lock:
            # Get the in-memory queue list for the specified queue name.
            # Use .get() with an empty list default for safety if the queue name is not found.
            queue = self.queues.get(queue_name, [])
            # Check if the queue list is not empty.
            if queue:
                # Remove and return the first job (highest priority) from the list using pop(0).
                return queue.pop(0)
            # Return None if the queue list was empty.
            return None

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously moves a failed job to the Dead Letter Queue (DLQ).

        Appends the failed job's payload dictionary to the in-memory DLQ list
        associated with the specified queue name. Updates the job's state to
        `State.FAILED` in the `job_states` dictionary. Access to the in-memory
        DLQ and job states is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: A dictionary containing the job data. Must include a unique
                     identifier (typically the 'id' field).
        """
        # Acquire the lock to ensure exclusive access to the in-memory DLQ and job states.
        async with self.lock:
            # Get the list for the DLQ associated with the queue name.
            # Use setdefault to create an empty list if it doesn't exist.
            dlq = self.dlqs.setdefault(queue_name, [])
            # Append the failed job payload dictionary to the DLQ list.
            dlq.append(payload)
            # Update the job's state to FAILED in the job_states dictionary.
            self.job_states[(queue_name, payload["id"])] = State.FAILED

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Acknowledges successful processing of a job.

        In this in-memory backend implementation, explicit acknowledgment via
        this method is not strictly necessary for managing job state transitions
        like COMPLETED or FAILED, as these updates are handled by the
        `update_job_state` method. This method primarily cleans up internal
        tracking of actively running jobs by removing the heartbeat and active
        job entries from memory. Access to these internal dictionaries is
        synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job being acknowledged.
        """
        # Acquire the lock to ensure exclusive access to heartbeats and active jobs.
        async with self.lock:
            # Create the job key tuple.
            job_key: _JobKey = (queue_name, job_id)
            # Remove the job's heartbeat entry from the heartbeats dictionary.
            # Use pop with None default to avoid KeyError if the key doesn't exist.
            self.heartbeats.pop(job_key, None)
            # Remove the job's entry from the active jobs dictionary.
            # Use pop with None default to avoid KeyError if the key doesn't exist.
            self.active_jobs.pop(job_key, None)

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Asynchronously schedules a job to be available for processing at a
        specific future time by adding it to the in-memory delayed list.

        Adds a tuple containing the scheduled `run_at` timestamp and the job
        payload dictionary to the in-memory `delayed` list associated with the
        specified queue name. Updates the job's state to `State.EXPIRED` in the
        `job_states` dictionary. Access to in-memory structures is synchronized
        using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: A dictionary containing the job data. Must include a unique
                     identifier (typically the 'id' field).
            run_at: The absolute timestamp (float, typically seconds since the
                    epoch) when the job should become available for processing.
        """
        # Acquire the lock to ensure exclusive access to the in-memory delayed list and job states.
        async with self.lock:
            # Get the list for delayed jobs associated with the queue name.
            # Use setdefault to create an empty list if it doesn't exist.
            delayed_list = self.delayed.setdefault(queue_name, [])
            # Append a tuple containing the run_at timestamp and the job payload to the list.
            delayed_list.append((run_at, payload))
            # Update the job's state to EXPIRED (or DELAYED) in the job_states dictionary.
            self.job_states[(queue_name, payload["id"])] = State.EXPIRED

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves delayed jobs from the specified queue that
        are now due for processing.

        Iterates through the in-memory delayed list for the given queue name.
        Identifies jobs whose scheduled `run_at` timestamp is less than or
        equal to the current time. These due jobs are removed from the in-memory
        delayed list, and their payloads are returned as a list. Jobs not yet
        due remain in the list. Access to the in-memory delayed list is
        synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of job data dictionaries for jobs that are due for processing.
        """
        # Acquire the lock to ensure exclusive access to the in-memory delayed list.
        async with self.lock:
            now = time.time()  # Get the current timestamp.
            due_jobs = []  # List to store job payloads that are due.
            still_delayed = []  # List to store (run_at, payload) tuples for jobs not yet due.
            # Iterate through the delayed jobs for the specified queue.
            # Use .get() with an empty list default for safety if the queue doesn't exist.
            for run_at, payload in self.delayed.get(queue_name, []):
                # Check if the job's scheduled run time is less than or equal to the current time.
                if run_at <= now:
                    due_jobs.append(payload)  # Add the job payload to the 'due_jobs' list.
                else:
                    still_delayed.append((run_at, payload))  # Keep the job in the 'still_delayed' list.
            # Update the in-memory delayed list for this queue name to contain only the jobs not yet due.
            self.delayed[queue_name] = still_delayed
            # Return the list of job payloads that were due.
            return due_jobs

    async def list_delayed(self, queue_name: str) -> list[DelayedInfo]:
        """
        Asynchronously returns a list of all jobs currently scheduled for future
        execution in the specified queue from the in-memory delayed list.

        Retrieves all (run_at, job_payload) tuples from the in-memory `delayed`
        list for the given queue, sorts them by the `run_at` timestamp, and
        converts them into a list of `DelayedInfo` dataclass instances. Access
        to the in-memory delayed list is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to list delayed jobs for.

        Returns:
            A list of `DelayedInfo` dataclass instances, sorted ascending by their
            `run_at` timestamp.
        """
        # Acquire the lock to ensure exclusive access to the in-memory delayed list.
        async with self.lock:
            out: list[DelayedInfo] = []  # Initialize an empty list to store the results.
            # Get the delayed jobs list for the specified queue, defaulting to an empty list.
            # Sort the list by the run_at timestamp (the first element of the tuple).
            for run_at, job in sorted(self.delayed.get(queue_name, []), key=lambda x: x[0]):
                # Append a new DelayedInfo instance to the output list.
                # job["id"] is used assuming the payload always has an 'id'.
                out.append(DelayedInfo(job_id=job["id"], run_at=run_at, payload=job))
            # Return the sorted list of DelayedInfo instances.
            return out

    async def remove_delayed(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously removes a specific job from the in-memory delayed queue
        by its ID.

        Iterates through the in-memory delayed list for the given queue name,
        finds the tuple containing the job whose payload dictionary has a
        matching 'id', removes it from the list, and updates the list in memory.
        Access to the in-memory delayed list is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove from the delayed queue.

        Returns:
            True if a job with the matching ID was found and removed from the
            delayed queue, False otherwise.
        """
        # Acquire the lock to ensure exclusive access to the in-memory delayed list.
        async with self.lock:
            # Get the current length of the delayed list before attempting removal.
            before = len(self.delayed.get(queue_name, []))
            # Filter the in-memory delayed list for the specified queue.
            # Keep only the (timestamp, job_payload) tuples where the job_payload's
            # 'id' does *not* match the job_id to be removed.
            # Use .get() with default [] for safety if the queue doesn't exist.
            self.delayed[queue_name] = [
                (ts, j) for ts, j in self.delayed.get(queue_name, []) if j.get("id") != job_id
            ]
            # Compare the length of the list after filtering to the length before filtering.
            # If the length is less, it means a job was removed, so return True.
            return len(self.delayed.get(queue_name, [])) < before

    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Asynchronously returns a list of all repeatable job definitions for a
        specific queue from the in-memory repeatable definitions dictionary.

        Retrieves all records from the in-memory `repeatables` dictionary
        associated with the given queue name. Deserializes the JSON string keys
        back into job definition dictionaries and converts the records into
        `RepeatableInfo` dataclass instances. The resulting list is sorted by
        the `next_run` timestamp. Access to the in-memory repeatable definitions
        is synchronized using the internal lock.

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

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously removes a specific repeatable job definition from the
        in-memory repeatable definitions dictionary.

        Serializes the provided job definition dictionary to a JSON string to use
        as the key in the in-memory dictionary. It then removes the corresponding
        entry from the `repeatables` dictionary for the given queue name. Access
        to the in-memory repeatable definitions is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to remove.
        """
        # Acquire the lock to ensure exclusive access to the in-memory repeatable definitions.
        async with self.lock:
            # Serialize the job definition dictionary to a JSON string.
            raw = json.dumps(job_def)
            # Remove the entry corresponding to the JSON string key from the repeatable definitions.
            # Use pop with None default to avoid KeyError if the key doesn't exist.
            self.repeatables.get(queue_name, {}).pop(raw, None)

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously marks a specific repeatable job definition as paused in
        the in-memory repeatable definitions dictionary.

        Serializes the provided job definition dictionary to a JSON string to use
        as the key. It then locates the corresponding record for this repeatable
        job under the specified queue name and updates its 'paused' flag to True.
        The scheduler logic relies on checking this flag. Access to the in-memory
        repeatable definitions is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to pause.
        """
        # Acquire the lock to ensure exclusive access to the in-memory repeatable definitions.
        async with self.lock:
            # Serialize the job definition dictionary to a JSON string.
            raw = json.dumps(job_def)
            # Get the inner dictionary of repeatable definitions for the specified queue.
            # Use .get() with an empty dictionary default for safety.
            queue_repeatables = self.repeatables.get(queue_name, {})
            # Get the specific record for the repeatable job using the JSON string key.
            rec = queue_repeatables.get(raw)
            # If the record exists (the repeatable job is found in memory).
            if rec:
                rec["paused"] = True  # Set the 'paused' flag to True.

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Asynchronously un-pauses a repeatable job definition in memory, computes
        its next scheduled run time, and updates the definition in memory.

        Serializes the provided job definition dictionary to a JSON string.
        Removes the old record (potentially marked as paused) from the in-memory
        repeatable definitions dictionary. Creates a 'clean' definition without
        the 'paused' flag, computes its next run time using `compute_next_run`,
        and adds a new record back into the in-memory dictionary with 'paused'
        set to False and the new `next_run` timestamp. Access to the in-memory
        repeatable definitions is synchronized using the internal lock.

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
            # Use pop() to remove and return the value, or None if not found.
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
        Asynchronously cancels a job.

        Removes the job from the in-memory waiting queue list and the in-memory
        delayed queue list based on its ID. Adds the job ID to the in-memory
        set of cancelled jobs for the given queue. Workers are expected to check
        this set and stop processing or skip the job if its ID is found here.
        Access to in-memory structures is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to cancel.
        """
        # Acquire the lock to ensure exclusive access to in-memory queues and cancelled set.
        async with self.lock:
            # Filter the in-memory waiting queue list for the specified queue.
            # Keep only the jobs whose payload 'id' does not match the job_id to be cancelled.
            # Use .get() with default [] for safety.
            self.queues[queue_name] = [
                j for j in self.queues.get(queue_name, []) if j.get("id") != job_id
            ]
            # Filter the in-memory delayed queue list for the specified queue.
            # Keep only the (timestamp, job_payload) tuples where the job payload's
            # 'id' does not match the job_id to be cancelled.
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
        set of cancelled jobs associated with the given `queue_name`. Workers
        typically use this method to determine if they should skip processing
        a job. Access to the in-memory cancelled set is synchronized using the
        internal lock.

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

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the processing state of a job in the in-memory
        `job_states` dictionary.

        This method updates the state string associated with the job key (a tuple
        of queue name and job ID) in the in-memory dictionary. Access to the
        `job_states` dictionary is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            state: A string representing the new state of the job (e.g., "active",
                   "completed", "failed", "waiting").
        """
        # Acquire the lock to ensure exclusive access to the in-memory job states dictionary.
        async with self.lock:
            # Update or add the state for the job key.
            self.job_states[(queue_name, job_id)] = state

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a completed job in the in-memory
        `job_results` dictionary.

        This method stores the execution result associated with the job key (a
        tuple of queue name and job ID) in the in-memory dictionary. Access to
        the `job_results` dictionary is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job whose result is to be saved.
            result: The result data of the job. Can be of any type.
        """
        # Acquire the lock to ensure exclusive access to the in-memory job results dictionary.
        async with self.lock:
            # Store the result for the job key.
            self.job_results[(queue_name, job_id)] = result

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Asynchronously retrieves the current processing state of a job from the
        in-memory `job_states` dictionary.

        This method retrieves the state string associated with the job key.
        Access to the `job_states` dictionary is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job whose state is requested.

        Returns:
            A string representing the job's state if found, otherwise None if the
            job key is not present in the `job_states` dictionary.
        """
        # Acquire the lock to ensure exclusive access to the in-memory job states dictionary.
        async with self.lock:
            # Return the state for the job key if it exists, otherwise None.
            return self.job_states.get((queue_name, job_id))

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Asynchronously retrieves the result data of a job from the in-memory
        `job_results` dictionary.

        This method retrieves the result data associated with the job key. Access
        to the `job_results` dictionary is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job whose result is requested.

        Returns:
            The job's result data if the job is found and has a result,
            otherwise None if the job key is not present in the `job_results`
            dictionary. The type of the result depends on what was saved
            by `save_job_result`.
        """
        # Acquire the lock to ensure exclusive access to the in-memory job results dictionary.
        async with self.lock:
            # Return the result for the job key if it exists, otherwise None.
            return self.job_results.get((queue_name, job_id))

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Asynchronously registers a job's dependencies in the in-memory dependency
        tracking dictionaries.

        Uses `self.deps_pending` to track which parent jobs a child job is waiting on
        (maps child job key to a set of parent IDs). Uses `self.deps_children` to
        track which child jobs are waiting on a parent job (maps parent job key
        to a set of child IDs). Access to these dictionaries is synchronized using
        the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The job data dictionary. Must contain at least an "id" key.
                      Can optionally include a "depends_on" key with a list of
                      parent job IDs (strings) that this job depends on.
        """
        # Acquire the lock to ensure exclusive access to the in-memory dependency dictionaries.
        async with self.lock:
            child_job_id = job_dict["id"]
            child_pend_key: _JobKey = (queue_name, child_job_id)
            # Get the set of parent job IDs this child depends on from the input dictionary.
            # Convert the input list to a set for efficient checking.
            parent_deps: set[str] = set(job_dict.get("depends_on", []))

            # If there are no dependencies specified, no further action is needed for this job.
            if not parent_deps:
                return

            # Store the set of parent IDs that this child job is waiting on in deps_pending.
            self.deps_pending[child_pend_key] = parent_deps
            # For each parent job ID that this child depends on.
            for parent_id in parent_deps:
                parent_children_key: _JobKey = (queue_name, parent_id)
                # Add the child job ID to the set of children waiting on this parent in deps_children.
                # setdefault ensures the set exists, creating it if necessary.
                self.deps_children.setdefault(parent_children_key, set()).add(child_job_id)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and checks if
        any dependent child jobs are now ready to be enqueued.

        Retrieves the list of child job IDs waiting on the `parent_id` from the
        in-memory `deps_children` dictionary. For each child, it removes the
        `parent_id` from the child's pending dependencies set in `deps_pending`.
        If a child job's pending dependencies set becomes empty, it means all its
        dependencies are met. The child job data is then fetched using `_fetch_job_data`,
        the child job is enqueued using `enqueue`, and a local 'job:ready' event
        is emitted. Finally, the empty pending dependencies entry for the child
        and the children tracking entry for the parent are cleaned up in memory.
        Access to in-memory structures is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the parent job belonged to.
            parent_id: The unique identifier of the job that just completed.
                       This parent's completion may resolve dependencies for other jobs.
        """
        # Acquire the lock to ensure exclusive access to in-memory dependency tracking dictionaries and queues.
        async with self.lock:
            parent_children_key: _JobKey = (queue_name, parent_id)
            # Get the set of child job IDs waiting on this parent from deps_children.
            # Convert to a list before iterating to avoid issues if the set is modified during iteration.
            children: list[str] = list(self.deps_children.get(parent_children_key, set()))

            # Iterate through each child job ID that was waiting on the parent.
            for child_id in children:
                child_pend_key: _JobKey = (queue_name, child_id)
                # Check if the child job is still being tracked in the pending dependencies dictionary.
                if child_pend_key in self.deps_pending:
                    # Remove the completed parent's ID from the child's pending dependencies set.
                    # discard() is used to avoid raising an error if the parent ID wasn't found (e.g., already resolved).
                    self.deps_pending[child_pend_key].discard(parent_id)
                    # Check if the child job now has no remaining pending dependencies.
                    if not self.deps_pending[child_pend_key]:
                        # If all dependencies are met, fetch the full job data for the child.
                        # Use the internal helper method to find the job data in the in-memory queues.
                        raw: dict[str, Any] | None = self._fetch_job_data(queue_name, child_id)
                        # If the job data was found in the in-memory queues.
                        if raw:
                            # Enqueue the child job into the main waiting queue.
                            # This will also update its state to WAITING.
                            await self.enqueue(queue_name, raw)
                            # Emit a local event indicating that the child job is now ready for processing.
                            # This can be useful for listeners or logging.
                            await event_emitter.emit("job:ready", {"id": child_id})
                        # Delete the child's entry from the pending dependencies dictionary as its dependencies are met.
                        del self.deps_pending[child_pend_key]
            # Remove the parent's entry from the children tracking dictionary as its dependencies are resolved.
            self.deps_children.pop(parent_children_key, None)

    async def pause_queue(self, queue_name: str) -> None:
        """
        Asynchronously marks the specified queue as paused in the in-memory
        `paused` set.

        Adding the queue name to this set signals to worker processes (that
        check `is_queue_paused`) that they should temporarily stop dequeueing
        new jobs from this queue. Access to the in-memory paused set is
        synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Add the queue name to the set of paused queues.
            self.paused.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Asynchronously marks the specified queue as resumed by removing its
        name from the in-memory `paused` set.

        Removing the queue name from this set signals to worker processes (that
        check `is_queue_paused`) that they can resume dequeueing jobs from
        this queue. Access to the in-memory paused set is synchronized using
        the internal lock.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Remove the queue name from the set of paused queues.
            # discard() is used instead of remove() to avoid raising a KeyError if the
            # queue was not in the set (i.e., was not paused).
            self.paused.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Asynchronously checks if the specified queue is currently marked as
        paused in the in-memory `paused` set.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue name is found in the in-memory `paused` set,
            indicating it is paused; False otherwise. Access to the in-memory
            paused set is synchronized using the internal lock.
        """
        # Acquire the lock to ensure exclusive access to the in-memory paused set.
        async with self.lock:
            # Check if the queue name is a member of the paused set.
            return queue_name in self.paused

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a specific job in the
        in-memory `job_progress` dictionary.

        This method stores the provided `progress` value (typically a float
        between 0.0 and 1.0 or an integer percentage) associated with the job
        key (a tuple of queue name and job ID) in the in-memory dictionary.
        This allows external components to monitor the progress of ongoing jobs
        within this backend instance. Access to the `job_progress` dictionary
        is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The progress value to save.
        """
        # Acquire the lock to ensure exclusive access to the in-memory job progress dictionary.
        async with self.lock:
            # Store the progress value for the job key.
            self.job_progress[(queue_name, job_id)] = progress

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue
        in a single batch operation in memory.

        Extends the in-memory queue list for the given queue name with the list
        of job payloads. Updates the state of each newly added job to
        `State.WAITING` in the `job_states` dictionary. The queue list is then
        sorted by job priority (lower `priority` value means higher priority).
        Access to in-memory structures is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue to enqueue jobs onto.
            jobs: A list of job payloads (dictionaries) to be enqueued. Each
                  dictionary must include a unique identifier (typically the 'id' field)
                  and can optionally include a 'priority' key.
        """
        # Acquire the lock to ensure exclusive access to the in-memory queues and job states.
        async with self.lock:
            # Get the list for the specified queue name.
            # Use setdefault to create an empty list if it doesn't exist.
            q = self.queues.setdefault(queue_name, [])
            # Extend the queue list with all the job payloads from the input list.
            q.extend(jobs)
            # Iterate through the newly added jobs to update their states.
            for job in jobs:
                # Update the job's state to WAITING in the job_states dictionary.
                # job["id"] is used assuming the 'id' key always exists.
                self.job_states[(queue_name, job["id"])] = State.WAITING
            # Sort the entire queue list based on job priority after adding all jobs.
            # Jobs with lower priority numbers will appear earlier in the list.
            # Defaults to priority 5 if the key is missing.
            q.sort(key=lambda job: job.get("priority", 5))

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Removes jobs from the in-memory backend based on their state and
        optional age criteria.

        Iterates through all job states stored in the `job_states` dictionary.
        If a job matches the specified queue name and state, its corresponding
        entries in the `job_states`, `job_results`, and `job_progress` dictionaries
        are removed. Note: The `older_than` parameter is accepted for compatibility
        but is not fully utilized in this in-memory implementation for filtering
        jobs based on age across all states, as timestamps like 'created_at' or
        'completed_at' are not consistently stored for all jobs in memory.
        Access to in-memory structures is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state string of the jobs to be removed (e.g., "completed",
                   "failed", "waiting").
            older_than: An optional timestamp (float). This parameter is not
                        fully utilized in this in-memory implementation for age-based
                        filtering across all job states. Defaults to None.
        """
        to_delete: list[_JobKey] = []  # List to store the keys of jobs to be deleted.
        # Iterate through a copy of the items in the job_states dictionary.
        # Using list() creates a copy to allow safe deletion during iteration.
        for (qname, jid), st in list(self.job_states.items()):
            # Check if the job's queue name and state match the specified criteria.
            if qname == queue_name and st == state:
                # If it matches, add the job key (queue name, job ID tuple) to the list of keys to delete.
                to_delete.append((qname, jid))

        # Iterate through the list of job keys that were identified for deletion.
        for key in to_delete:
            # Remove the job's state from the job_states dictionary. Use pop with None default.
            self.job_states.pop(key, None)
            # Remove the job's result from the job_results dictionary. Use pop with None default.
            self.job_results.pop(key, None)
            # Remove the job's progress from the job_progress dictionary. Use pop with None default.
            self.job_progress.pop(key, None)

        # Note: This purge logic only removes entries from state/result/progress dictionaries.
        # It does not remove the job payloads from the actual in-memory queue (self.queues),
        # delayed (self.delayed), or DLQ (self.dlqs) lists. Purging from these lists
        # would require iterating and removing from them as well.

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Asynchronously emits a backend-specific lifecycle event using the global
        event emitter.

        This allows the in-memory backend to signal various events (e.g., job
        status changes) which can be observed by local listeners within the
        same process.

        Args:
            event: A string representing the name of the event to emit.
            data: A dictionary containing data associated with the event.
        """
        # Emit the event using the core event emitter utility.
        await event_emitter.emit(event, data)

    async def create_lock(self, key: str, ttl: int) -> anyio.Lock:
        """
        Creates and returns an `anyio.Lock` instance for synchronization within
        this single process.

        This method returns a standard `anyio.Lock`. It's important to note that
        `anyio.Lock` provides synchronization for tasks running concurrently
        within the same process or thread group using `anyio`, but it *does not*
        provide a true distributed lock across multiple processes or machines.
        The `key` and `ttl` arguments are part of the `BaseBackend` interface
        but are not directly used by the `anyio.Lock`.

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
        Atomically enqueues multiple jobs and registers their dependencies
        within this in-memory backend instance.

        This method is intended to enqueue a batch of jobs and set up their
        dependencies such that the dependencies are registered before the jobs
        are made available for processing, ensuring a 'flow' structure.
        However, this specific implementation in the `InMemoryBackend` raises
        a `NotImplementedError`, indicating that atomic flow creation is not
        supported by this in-memory backend.

        Args:
            queue_name: The target queue for all jobs in the flow.
            job_dicts: A list of job payloads (dictionaries) to enqueue.
            dependency_links: A list of tuples, where each tuple is
                              (parent_job_id, child_job_id), defining the
                              dependencies within the flow.

        Returns:
            A list of job IDs that were successfully enqueued (this method
            currently raises an error before returning).

        Raises:
            NotImplementedError: This method is not implemented for the
                                 `InMemoryBackend`.
        """
        # This operation is not supported by the InMemoryBackend, so raise an error.
        raise NotImplementedError("atomic_add_flow not supported for InMemoryBackend")

    def _fetch_job_data(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        """
        Helper method to find a job's data dictionary in the in-memory queues.

        This method iterates through the job payloads stored in the in-memory
        waiting queue list (`self.queues`) for the specified queue name to
        locate the job dictionary with a matching 'id'.

        Note: This linear search approach can be inefficient for queues with
        a large number of waiting jobs. It's used internally, for example,
        during dependency resolution to retrieve the full job data before
        enqueuing a child job. Access to the in-memory queue is assumed to be
        handled by the caller acquiring the lock if necessary.

        Args:
            queue_name: The name of the queue to search for the job.
            job_id: The unique identifier of the job to find.

        Returns:
            The job data dictionary if a job with the matching ID is found
            in the waiting queue, otherwise None.
        """
        # Get the list of job payloads for the specified queue, defaulting to an empty list.
        # Iterate through each job dictionary in this list.
        for job in self.queues.get(queue_name, []):
            # Check if the job's 'id' field matches the target job_id.
            # Use .get("id") for safety in case the 'id' key is unexpectedly missing.
            if job.get("id") == job_id:
                return job  # Return the job data dictionary if a match is found.
        # If the loop completes without finding a matching job ID, return None.
        return None

    async def list_queues(self) -> list[str]:
        """
        Asynchronously lists all known queue names currently managed by this
        in-memory backend instance.

        This method returns a list of all keys (queue names) that exist in the
        in-memory `self.queues` dictionary, which stores the lists of waiting jobs.
        Access to the in-memory queues dictionary is synchronized using the internal lock.

        Returns:
            A list of strings, where each string is the name of a queue that
            has had jobs enqueued into it within this backend instance's lifetime.
            The list reflects the current state of the `self.queues` keys.
        """
        # Acquire the lock to ensure exclusive access to the in-memory queues dictionary.
        async with self.lock:
            # Return a list of all keys (queue names) currently present in the self.queues dictionary.
            return list(self.queues.keys())

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Asynchronously returns statistics about the number of jobs currently
        present in different states for a specific queue within the in-memory
        backend.

        Provides counts for jobs currently in the waiting queue (`self.queues`),
        the delayed queue (`self.delayed`), and the dead letter queue (DLQ)
        (`self.dlqs`). The 'waiting' count is calculated by subtracting the
        number of jobs in the DLQ from the raw count in the waiting queue list,
        to account for jobs that might logically be considered 'failed' but still
        reside in the waiting queue list. Access to in-memory queues is synchronized
        using the internal lock.

        Args:
            queue_name: The name of the queue to get statistics for.

        Returns:
            A dictionary containing the counts for "waiting", "delayed", and
            "failed" jobs. Counts are defaulted to 0 if no jobs are found in a
            specific state or if the queue name does not exist in the respective
            in-memory dictionaries.
        """
        # Acquire the lock to ensure exclusive access to the in-memory queues, delayed, and DLQ dictionaries.
        async with self.lock:
            # Get the raw count of jobs in the in-memory waiting queue list.
            # Use .get() with default [] for safety.
            raw_waiting = len(self.queues.get(queue_name, []))
            # Get the count of jobs in the in-memory delayed queue list.
            # Use .get() with default [] for safety.
            delayed = len(self.delayed.get(queue_name, []))
            # Get the count of jobs in the in-memory dead letter queue list.
            # Use .get() with default [] for safety.
            failed = len(self.dlqs.get(queue_name, []))

            # Calculate the effective waiting count. Subtract the count of failed jobs
            # from the raw waiting count. This is a heuristic for in-memory where
            # a job might be in the waiting list but logically considered failed.
            # Ensure the result is not negative using max().
            waiting = raw_waiting - failed
            if waiting < 0:
                waiting = 0

        # Return a dictionary containing the calculated statistics for each state.
        return {
            "waiting": waiting,
            "delayed": delayed,
            "failed": failed,
        }

    async def list_jobs(self, queue_name: str, state: str) -> list[dict[str, Any]]:
        """
        Asynchronously lists job payloads from the in-memory backend filtered by
        their conceptual state.

        Retrieves job payloads from the relevant in-memory list based on the
        specified `state` ('waiting', 'delayed', or 'failed'). Note that this
        method only retrieves jobs from the primary in-memory lists and does
        not query based on the `job_states` dictionary, which tracks the
        processing status.

        Args:
            queue_name: The name of the queue to list jobs from.
            state: The conceptual job state to filter by. Supported values:
                   "waiting", "delayed", "failed".

        Returns:
            A list of job data dictionaries for jobs found in the corresponding
            in-memory list for the specified queue and state. Returns an empty
            list for unsupported states or if the queue/state combination is empty.
            Access to in-memory structures is synchronized using the internal lock.
        """
        # Acquire the lock to ensure exclusive access to the in-memory queues, delayed, and DLQ.
        async with self.lock:
            # Use a match statement to handle different states.
            match state:
                case "waiting":
                    # Return a list of job payloads from the in-memory waiting queue list.
                    # Use .get() with default [] for safety. Use list() to return a copy.
                    return list(self.queues.get(queue_name, []))
                case "delayed":
                    # Return a list of job payloads from the in-memory delayed queue list.
                    # Iterate through (timestamp, job_payload) tuples and extract only the payload.
                    # Use .get() with default [] for safety.
                    return [job for _, job in self.delayed.get(queue_name, [])]
                case "failed":
                    # Return a list of job payloads from the in-memory dead letter queue list.
                    # Use .get() with default [] for safety. Use list() to return a copy.
                    return list(self.dlqs.get(queue_name, []))
                case _:
                    # Return an empty list for any unsupported state.
                    return []

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously attempts to retry a job currently in the Dead Letter
        Queue (DLQ) in memory.

        Searches the in-memory DLQ list for the specified queue name to find the
        job with a matching ID. If found, the job is removed from the DLQ list,
        added back to the main waiting queue list, and its state is updated to
        `State.WAITING` in the `job_states` dictionary. Access to in-memory
        structures is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the failed job to retry.

        Returns:
            True if the job was found in the DLQ and successfully moved back
            to the waiting queue, False otherwise.
        """
        # Acquire the lock to ensure exclusive access to in-memory DLQ, queues, and job states.
        async with self.lock:
            # Get the in-memory DLQ list for the specified queue, defaulting to an empty list.
            dlq = self.dlqs.get(queue_name, [])
            # Iterate through the job payloads in the DLQ list.
            # Use list() to iterate over a copy to allow modification of the original list during iteration.
            for job in list(dlq):
                # Check if the job's 'id' field matches the job_id to retry.
                # job["id"] is used assuming the 'id' key always exists.
                if job["id"] == job_id:
                    # Remove the job from the DLQ list.
                    try:
                        dlq.remove(job)
                    except ValueError:
                        # Handle case where job might have been removed concurrently.
                        pass
                    # Get the in-memory waiting queue list for the specified queue, creating it if necessary.
                    main_queue = self.queues.setdefault(queue_name, [])
                    # Add the job back to the end of the main waiting queue list.
                    main_queue.append(job)
                    # Update the job's state to WAITING in the job_states dictionary.
                    self.job_states[(queue_name, job_id)] = State.WAITING
                    return True  # Indicate that the job was found and retried.
            # Return False if the job with the given ID was not found in the DLQ.
            return False

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously removes a specific job from any of the main in-memory
        queues (waiting, delayed, or DLQ) and cleans up its associated state,
        result, and progress information.

        This method iterates through the in-memory lists for waiting, delayed,
        and DLQ jobs for the given queue name. It finds and removes the job
        with a matching ID from whichever list it is found in. It also removes
        the job's entries from the in-memory `job_states`, `job_results`, and
        `job_progress` dictionaries. Access to in-memory structures is synchronized
        using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove.

        Returns:
            True if a job with the given ID and queue name was found and removed
            from any of the in-memory queues or associated dictionaries, False otherwise.
        """
        async with self.lock:
            removed = False  # Flag to track if the job was removed from any list or dict.
            job_key: _JobKey = (queue_name, job_id)

            # Try to remove from the in-memory waiting queue list.
            # Get the list, defaulting to empty for safety.
            waiting_queue = self.queues.get(queue_name, [])
            # Iterate over a copy to allow modification.
            for job in list(waiting_queue):
                if job.get("id") == job_id:
                    try:
                        waiting_queue.remove(job)
                        removed = True
                    except ValueError:
                        pass  # Already removed concurrently

            # Try to remove from the in-memory delayed queue list.
            # Get the list, defaulting to empty for safety.
            delayed_list = self.delayed.get(queue_name, [])
            # Iterate over a copy to allow modification.
            for ts, job in list(delayed_list):
                if job.get("id") == job_id:
                    try:
                        delayed_list.remove((ts, job))
                        removed = True
                    except ValueError:
                        pass  # Already removed concurrently

            # Try to remove from the in-memory DLQ list.
            # Get the list, defaulting to empty for safety.
            dlq_list = self.dlqs.get(queue_name, [])
            # Iterate over a copy to allow modification.
            for job in list(dlq_list):
                if job.get("id") == job_id:
                    try:
                        dlq_list.remove(job)
                        removed = True
                    except ValueError:
                        pass  # Already removed concurrently

            # Remove from job states, results, and progress dictionaries if they exist.
            if job_key in self.job_states:
                del self.job_states[job_key]
                removed = True
            if job_key in self.job_results:
                del self.job_results[job_key]
                removed = True
            if job_key in self.job_progress:
                del self.job_progress[job_key]
                removed = True
            # Note: Also consider removing from self.heartbeats, self.active_jobs,
            # self.deps_pending, self.deps_children, self.cancelled if a job
            # can exist there without being in queues/delayed/dlq lists.
            if job_key in self.heartbeats:
                del self.heartbeats[job_key]
                removed = True
            if job_key in self.active_jobs:
                del self.active_jobs[job_key]
                removed = True
            if job_key in self.deps_pending:
                del self.deps_pending[job_key]
                # Note: Need to iterate through deps_children to remove the job_id
                # from any parent's children set if this job was a child.
                removed = True

            # Clean up from deps_children where this job was a child
            for parent_key in list(self.deps_children.keys()): # Iterate over copy
                 if job_id in self.deps_children.get(parent_key, set()):
                     self.deps_children[parent_key].discard(job_id)
                     if not self.deps_children[parent_key]:
                         del self.deps_children[parent_key]


            # Clean up from cancelled set
            if queue_name in self.cancelled and job_id in self.cancelled[queue_name]:
                 self.cancelled[queue_name].discard(job_id)
                 if not self.cancelled[queue_name]:
                     del self.cancelled[queue_name]
                 removed = True


            return removed  # Return True if the job was removed anywhere, False otherwise.

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Records or updates the last heartbeat timestamp for a specific job
        in the in-memory `heartbeats` dictionary.

        This method acquires an exclusive lock to safely update the in-memory
        `heartbeats` dictionary, associating the provided `timestamp` with the
        given job, identified by its queue name and job ID. This information
        is used by the stalled job detection mechanism. Access to the `heartbeats`
        dictionary is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            timestamp: The Unix timestamp (float) representing the time of the heartbeat.
        """
        # Acquire the lock to ensure safe concurrent modification of self.heartbeats
        async with self.lock:
            # Store the heartbeat timestamp. The key is a tuple combining
            # queue name and job ID for uniqueness.
            self.heartbeats[(queue_name, job_id)] = timestamp

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Retrieves a list of jobs currently tracked by heartbeats that have not
        sent a heartbeat since a specified timestamp.

        This method iterates through all recorded heartbeats in the in-memory
        `heartbeats` dictionary. If a job's heartbeat timestamp is older than the
        `older_than` threshold, it's considered stalled. The method then attempts
        to find the corresponding job's full data payload within the in-memory
        waiting queues (`self.queues`) using the `_fetch_job_data` helper method.
        Access to both `heartbeats` and `queues` is synchronized using the internal lock.

        Args:
            older_than: A Unix timestamp (float). Any job with a heartbeat timestamp
                        strictly less than this value will be considered stalled.

        Returns:
            A list of dictionaries. Each dictionary contains 'queue_name' (string)
            and 'job_data' (the original dictionary payload of the job) for each
            stalled job found that also exists in the in-memory waiting queue.
        """
        stalled: list[dict[str, Any]] = []  # Initialize an empty list to collect stalled job details

        # Acquire the lock to ensure safe concurrent access to both self.heartbeats
        # and self.queues (via _fetch_job_data) during the check and lookup process.
        async with self.lock:
            # Iterate over a copy of the heartbeats items. Using list() creates a copy
            # to avoid potential issues if heartbeats were modified during iteration
            # (though modifications are limited by the lock here, copying is a robust pattern).
            for (q, jid), ts in list(self.heartbeats.items()):
                # Check if the job's last heartbeat timestamp is older than the threshold.
                if ts < older_than:
                    # The job is potentially stalled. Try to find its full job payload
                    # in the relevant waiting queue using the internal helper method.
                    payload: dict[str, Any] | None = self._fetch_job_data(q, jid)
                    # If a corresponding job payload was found in the in-memory queue.
                    if payload:
                        # Add a dictionary containing the queue name and job data to the stalled list.
                        stalled.append({"queue_name": q, "job_data": payload})

        return stalled  # Return the list of identified stalled jobs

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        """
        Re-enqueues a job that was previously identified as stalled back onto
        its waiting queue in memory and removes its associated heartbeat entry.

        This method assumes the job is being moved from a "running" or "stalled"
        conceptual state back to a "waiting" state. It appends the job's data
        back to the end of the specified queue in the in-memory `self.queues` dictionary.
        It also removes the job's entry from the in-memory `self.heartbeats` dictionary,
        signifying it's no longer actively being tracked by heartbeat. Access to both
        the in-memory queue and heartbeats is synchronized using the internal lock.

        Args:
            queue_name: The name of the queue the job should be added back into.
            job_data: The dictionary containing the job's data payload. This
                      dictionary must contain an 'id' key to identify the job
                      for heartbeat removal.
        """
        # Acquire the lock to ensure safe concurrent modification of both self.queues
        # and self.heartbeats.
        async with self.lock:
            # Append the job data back to the list associated with the queue_name.
            # setdefault(queue_name, []) ensures that if the queue_name does not
            # already exist as a key in self.queues, it is created with an empty
            # list as its value before appending the job_data.
            self.queues.setdefault(queue_name, []).append(job_data)

            # Remove the heartbeat entry for this specific job from the heartbeats
            # dictionary. This stops tracking its heartbeat as it's now re-enqueued.
            # Use .pop() with a default of None to prevent a KeyError if the entry
            # was somehow already removed (e.g., by another process or cleanup task).
            # Accessing job_data["id"] assumes 'id' key exists.
            self.heartbeats.pop((queue_name, job_data["id"]), None)
