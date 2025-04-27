from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseBackend(ABC):
    """
    Abstract base class defining the contract for a queue backend implementation.

    A concrete backend must inherit from this class and implement all the
    abstract methods. This interface covers core queue operations (enqueueing,
    dequeueing, dead-letter queue), delayed jobs, job state management,
    dependency tracking, queue pause/resume functionality, bulk operations,
    cleanup, event emission, and distributed locking.
    """
    @abstractmethod
    async def enqueue(self, queue_name: str, payload: Dict[str, Any]) -> None:
        """
        Asynchronously enqueues a job payload onto the specified queue for
        immediate processing.

        Args:
            queue_name: The name of the queue to enqueue the job onto.
            payload: The job data as a dictionary.
        """
        ...

    @abstractmethod
    async def dequeue(self, queue_name: str) -> Optional[Dict[str, Any]]:
        """
        Asynchronously attempts to dequeue a job from the specified queue.

        This method should block or wait until a job is available or a timeout
        occurs (though the timeout is often handled by the consumer logic
        polling this method).

        Args:
            queue_name: The name of the queue to dequeue a job from.

        Returns:
            The job data as a dictionary if a job was successfully dequeued,
            otherwise None.
        """
        ...

    @abstractmethod
    async def move_to_dlq(self, queue_name: str, payload: Dict[str, Any]) -> None:
        """
        Asynchronously moves a job payload to the Dead Letter Queue (DLQ)
        associated with the specified queue.

        This is typically used for jobs that have permanently failed (exhausted
        retries) or expired.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: The job data as a dictionary to be moved to the DLQ.
        """
        ...

    @abstractmethod
    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        This signals the backend to remove the job from the active processing
        state and potentially clean it up or move it to a completed state.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job being acknowledged.
        """
        ...

    @abstractmethod
    async def enqueue_delayed(self, queue_name: str, payload: Dict[str, Any], run_at: float) -> None:
        """
        Asynchronously schedules a job to be available for processing at a
        specific future time.

        The job should not be delivered to workers until `run_at` is reached.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: The job data as a dictionary.
            run_at: The absolute timestamp (e.g., from time.time()) when the
                    job should become available for processing.
        """
        ...

    @abstractmethod
    async def get_due_delayed(self, queue_name: str) -> List[Dict[str, Any]]:
        """
        Asynchronously retrieves a list of delayed job payloads from the
        specified queue that are now due for processing (i.e., their
        `run_at` timestamp is in the past).

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of dictionaries, where each dictionary is a job payload
            that is ready to be moved to the main queue.
        """
        ...

    @abstractmethod
    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously removes a job from the backend's delayed storage.

        This is typically called after a delayed job has been retrieved by
        the scanner and moved to the main queue.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove from delayed storage.
        """
        ...

    @abstractmethod
    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the status of a specific job in the backend storage.

        This is used to reflect the job's current lifecycle state (e.g., "active",
        "completed", "failed", "delayed").

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            state: The new state string for the job (e.g., "active", "completed").
        """
        ...

    @abstractmethod
    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a job's execution in the backend storage.

        This is called after a job has completed successfully.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            result: The result returned by the job's task function.
        """
        ...

    @abstractmethod
    async def get_job_state(self, queue_name: str, job_id: str) -> Optional[str]:
        """
        Asynchronously retrieves the current status string of a specific job.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The job's status string if found, otherwise None.
        """
        ...

    @abstractmethod
    async def get_job_result(self, queue_name: str, job_id: str) -> Optional[Any]:
        """
        Asynchronously retrieves the execution result of a specific job.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The result of the job's task function if the job completed
            successfully and the result was saved, otherwise None.
        """
        ...

    @abstractmethod
    async def add_dependencies(self, queue_name: str, job_dict: Dict[str, Any]) -> None:
        """
        Asynchronously registers a job's dependencies and the relationship
        between dependent jobs and this job.

        This is called when a job with a `depends_on` list is created. The
        backend must track these dependencies to ensure dependent jobs are
        not executed until their requirements are met.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The job data dictionary, containing the 'depends_on' list.
        """
        ...

    @abstractmethod
    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals the backend that a parent job has completed,
        triggering the resolution of dependencies.

        The backend should check for any jobs that depended on `parent_id`
        and determine if all dependencies for those jobs are now satisfied.
        Satisfied jobs should be moved to a ready state or enqueued.

        Args:
            queue_name: The name of the queue the parent job belonged to.
            parent_id: The unique identifier of the job that just completed.
        """
        ...

    @abstractmethod
    async def pause_queue(self, queue_name: str) -> None:
        """
        Asynchronously signals the backend to pause job consumption for the
        specified queue.

        Workers polling this queue should stop dequeueing new jobs.

        Args:
            queue_name: The name of the queue to pause.
        """
        ...

    @abstractmethod
    async def resume_queue(self, queue_name: str) -> None:
        """
        Asynchronously signals the backend to resume job consumption for the
        specified queue if it was paused.

        Args:
            queue_name: The name of the queue to resume.
        """
        ...

    @abstractmethod
    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Asynchronously checks if the specified queue is currently paused.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue is paused, False otherwise.
        """
        ...

    @abstractmethod
    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a specific job.

        This is used by tasks that report their progress.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The progress value, typically a float between 0.0 and 1.0.
        """
        ...

    @abstractmethod
    async def bulk_enqueue(self, queue_name: str, jobs: List[Dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue
        in a single batch operation.

        Args:
            queue_name: The name of the queue to enqueue jobs onto.
            jobs: A list of job payloads (dictionaries) to be enqueued.
        """
        ...

    @abstractmethod
    async def purge(self, queue_name: str, state: str, older_than: Optional[float] = None) -> None:
        """
        Asynchronously removes jobs from a queue based on their state and
        optional age criteria.

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state of the jobs to be removed (e.g., "completed", "failed").
            older_than: An optional timestamp. Only jobs in the specified state
                        whose relevant timestamp is older than this will be purged.
                        If None, all jobs in the state might be purged.
        """
        ...

    @abstractmethod
    async def emit_event(self, event: str, data: Dict[str, Any]) -> None:
        """
        Asynchronously broadcasts an event to distributed listeners via the backend.

        This is used for system-wide events or monitoring.

        Args:
            event: The name of the event to emit.
            data: The data associated with the event.
        """
        ...

    @abstractmethod
    async def create_lock(self, key: str, ttl: int) -> Any:
        """
        Asynchronously creates or retrieves a distributed lock object for a
        given key with a specified time-to-live.

        The returned object should provide `acquire()` and `release()` methods
        that implement the distributed locking logic. The specific type of the
        returned lock object is backend-dependent.

        Args:
            key: A unique string identifier for the lock.
            ttl: The time-to-live for the lock in seconds.

        Returns:
            A backend-specific object representing the distributed lock.
        """
        ...
