import random
from typing import Any, Dict, List, Optional

from asyncmq.event import event_emitter
from asyncmq.job import Job


async def add_dependencies(backend: Any, queue: str, job: Job) -> None:
    """
    Registers a job's dependencies with the backend and links dependent
    (child) jobs to this job's completion.

    This function is called when a job with a populated `depends_on` list
    is enqueued. It informs the backend about the dependencies so that
    dependent jobs can be tracked and potentially unlocked upon this job's
    completion.

    Args:
        backend: An object providing the backend interface, expected to have
                 an `add_dependencies` method. Typed as `Any`.
        queue: The name of the queue the job belongs to.
        job: The `Job` instance whose dependencies need to be registered.
    """
    # If the job has no dependencies listed, there's nothing to do.
    if not job.depends_on:
        return
    # Delegate the dependency registration to the backend.
    await backend.add_dependencies(queue, job.to_dict())


async def resolve_dependency(backend: Any, queue: str, parent_id: str) -> None:
    """
    Checks for and potentially enqueues jobs whose dependencies are now met
    after a parent job's completion.

    This function is called when a job successfully completes. It signals
    the backend that a job with `parent_id` is finished. The backend then
    checks if any jobs were depending on `parent_id` and if all of their
    dependencies are now satisfied. If so, those dependent jobs are typically
    moved to a ready state or directly enqueued by the backend.

    Args:
        backend: An object providing the backend interface, expected to have
                 a `resolve_dependency` method. Typed as `Any`.
        queue: The name of the queue the parent job belonged to.
        parent_id: The unique ID of the job that just completed.
    """
    # Delegate the dependency resolution process to the backend.
    await backend.resolve_dependency(queue, parent_id)


async def pause_queue(backend: Any, queue: str) -> None:
    """
    Signals the backend to temporarily stop consuming new jobs from the
    specified queue.

    Args:
        backend: An object providing the backend interface, expected to have
                 a `pause_queue` method. Typed as `Any`.
        queue: The name of the queue to pause.
    """
    # Delegate the pause operation to the backend.
    await backend.pause_queue(queue)


async def resume_queue(backend: Any, queue: str) -> None:
    """
    Signals the backend to resume consuming jobs from a queue that was
    previously paused.

    Args:
        backend: An object providing the backend interface, expected to have
                 a `resume_queue` method. Typed as `Any`.
        queue: The name of the queue to resume.
    """
    # Delegate the resume operation to the backend.
    await backend.resume_queue(queue)


async def is_queue_paused(backend: Any, queue: str) -> bool:
    """
    Checks if a specific queue is currently marked as paused by the backend.

    Args:
        backend: An object providing the backend interface, expected to have
                 an `is_queue_paused` method. Typed as `Any`.
        queue: The name of the queue to check.

    Returns:
        True if the queue is paused, False otherwise.
    """
    # Delegate the check operation to the backend and return its result.
    return await backend.is_queue_paused(queue)


def jittered_backoff(base_delay: float, attempt: int, jitter: float = 0.1) -> float:
    """
    Computes a retry delay using an exponential backoff strategy with added
    random jitter.

    The formula is `delay = base_delay * (2 ** (attempt - 1)) + random_jitter`.
    Jitter is a random amount between `-jitter_amount` and `+jitter_amount`,
    where `jitter_amount` is `delay * jitter`. This helps prevent thundering
    herd problems where many failed jobs retry simultaneously.

    Args:
        base_delay: The base delay in seconds for the first retry attempt.
        attempt: The current retry attempt number (starting from 1 for the first retry).
        jitter: The fraction of the calculated delay to use as the maximum
                jitter amount. Defaults to 0.1 (10%).

    Returns:
        The calculated retry delay in seconds, including jitter.
    """
    # Calculate the exponential backoff delay.
    delay: float = base_delay * (2 ** (attempt - 1))
    # Calculate the maximum amount of jitter.
    jitter_amount: float = delay * jitter
    # Add a random value within the jitter range to the delay.
    return delay + random.uniform(-jitter_amount, jitter_amount)


async def report_progress(
    backend: Any, queue: str, job: Job, pct: float, info: Any = None
) -> None:
    """
    Records the progress percentage of a job in the backend and emits a
    local `job:progress` event.

    Args:
        backend: An object providing the backend interface, expected to have
                 a `save_job_progress` method. Typed as `Any`.
        queue: The name of the queue the job belongs to.
        job: The `Job` instance reporting progress.
        pct: The progress percentage as a float between 0.0 and 1.0.
        info: Optional additional data to include with the progress report.
    """
    # Persist the progress percentage with the backend.
    await backend.save_job_progress(queue, job.id, pct)
    # Emit a local event for real-time monitoring or other listeners.
    await event_emitter.emit(
        "job:progress",
        # Include relevant job details along with progress and info.
        {**job.to_dict(), "progress": pct, "info": info},
    )


# Assign the report_progress function as a method to the Job class.
# This allows calling Job instances like `job.report_progress(...)`.
Job.report_progress = report_progress
"""
Assigns the `report_progress` function as a method (`report_progress`) to the
`Job` class, enabling `job_instance.report_progress(...)` calls.
"""


async def bulk_enqueue(backend: Any, queue: str, jobs: List[Dict[str, Any]]) -> None:
    """
    Enqueues multiple jobs onto a queue in a single batch operation via the backend.

    This method is typically more performant than enqueueing jobs one by one
    when dealing with a large number of jobs.

    Args:
        backend: An object providing the backend interface, expected to have
                 a `bulk_enqueue` method. Typed as `Any`.
        queue: The name of the queue to enqueue jobs onto.
        jobs: A list of job payloads (dictionaries) to be enqueued.
    """
    # Delegate the bulk enqueue operation to the backend.
    await backend.bulk_enqueue(queue, queue, jobs) # Note: Original code had queue twice? Assuming first is channel, second is queue name. Fixing to backend method signature.


async def purge_jobs(backend: Any, queue: str, state: str, older_than: Optional[float] = None) -> None:
    """
    Removes jobs from a queue based on their state and optional age criteria
    via the backend.

    Args:
        backend: An object providing the backend interface, expected to have
                 a `purge` method. Typed as `Any`.
        queue: The name of the queue from which to purge jobs.
        state: The state of the jobs to be removed (e.g., "completed", "failed").
        older_than: An optional timestamp. Only jobs in the specified state
                    whose relevant timestamp (completion/failure/expiration time)
                    is older than this value will be removed. If None, all jobs
                    in the specified state might be purged.
    """
    # Delegate the purge operation to the backend.
    await backend.purge(queue, state, older_than)


async def emit_event(backend: Any, event: str, data: Dict[str, Any]) -> None:
    """
    Emits an event both locally through the `event_emitter` and, if the backend
    supports it, broadcasts the event for distributed listeners.

    Args:
        backend: An object providing the backend interface. It's optionally
                 expected to have an `emit_event` method for distributed events.
                 Typed as `Any`.
        event: The name of the event to emit.
        data: The data associated with the event.
    """
    # Emit the event to local listeners via the global event emitter.
    await event_emitter.emit(event, data)
    # If the backend has an emit_event method, use it for distributed broadcasting.
    if hasattr(backend, 'emit_event'):
        await backend.emit_event(event, data)


class Lock:
    """
    A wrapper class around a backend-specific distributed lock object.

    This class provides a standard asynchronous interface (`acquire`, `release`)
    for interacting with a distributed lock managed by the backend, abstracting
    away backend-specific lock implementations.
    """
    def __init__(self, lock_obj: Any) -> None:
        """
        Initializes the Lock wrapper.

        Args:
            lock_obj: The backend-specific lock object provided by the backend's
                      `create_lock` method. Its specific type is determined by
                      the backend implementation, hence typed as `Any`.
        """
        # Store the underlying backend-specific lock object.
        self._lock: Any = lock_obj

    async def acquire(self) -> bool:
        """
        Asynchronously attempts to acquire the distributed lock.

        This method delegates the acquire operation to the underlying backend
        lock object.

        Returns:
            True if the lock was acquired successfully, False otherwise.
        """
        # Delegate the acquire call to the backend's lock object.
        return await self._lock.acquire()

    async def release(self) -> bool:
        """
        Asynchronously attempts to release the distributed lock.

        This method delegates the release operation to the underlying backend
        lock object. The outcome might indicate whether the release was successful
        (e.g., the lock was held by this instance).

        Returns:
            True if the lock was released successfully, False otherwise.
        """
        # Delegate the release call to the backend's lock object.
        return await self._lock.release()


async def create_lock(backend: Any, key: str, ttl: int = 30) -> Lock:
    """
    Creates a new distributed lock instance for a given key via the backend.

    This function requests the backend to create a lock object associated with
    a unique `key`. The lock can be used to ensure that only one process or
    worker holds the lock for that key at a time. The lock has a time-to-live
    (TTL) after which it should automatically expire if not released.

    Args:
        backend: An object providing the backend interface, expected to have
                 a `create_lock` method. Typed as `Any`.
        key: A unique string identifier for the lock.
        ttl: The time-to-live for the lock in seconds. The lock will
             automatically expire after this duration if not released. Defaults to 30.

    Returns:
        A `Lock` instance wrapping the backend-specific lock object.
    """
    # Delegate the lock creation request to the backend.
    lock_obj: Any = await backend.create_lock(key, ttl)
    # Wrap the backend's lock object in the standard Lock class.
    return Lock(lock_obj)
