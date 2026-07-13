from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import asyncmq
from asyncmq.core.enums import State
from asyncmq.core.inspection import paginate_jobs, sanitize_job_types

if TYPE_CHECKING:
    from asyncmq.conf.global_settings import Settings
    from asyncmq.core.json_serializer import JSONSerializer


@dataclass
class RepeatableInfo:
    """
    Dataclass representing information about a repeatable job definition.

    Attributes:
        job_def: A dictionary containing the definition of the repeatable job.
        next_run: The timestamp (float) when the next run of this job is scheduled.
        paused: A boolean indicating whether this repeatable job is currently paused.
    """

    job_def: dict[str, Any]
    next_run: float
    paused: bool


@dataclass
class DelayedInfo:
    """
    Dataclass representing information about a delayed job.

    Attributes:
        job_id: The unique identifier of the delayed job.
        run_at: The timestamp (float) when the delayed job is scheduled to run.
        payload: A dictionary containing the full job payload for the delayed job.
    """

    job_id: str
    run_at: float
    payload: dict[str, Any]


@dataclass
class WorkerInfo:
    """
    Information about a worker (for dashboard display).
    """

    id: str
    queue: str
    concurrency: int
    heartbeat: float


class BaseBackend(ABC):
    """
    Abstract base class defining the contract for a queue backend implementation.

    A concrete backend must inherit from this class and implement all the
    abstract methods. This interface covers core queue operations (enqueueing,
    dequeueing, dead-letter queue), delayed jobs, job state management,
    dependency tracking, queue pause/resume functionality, bulk operations,
    cleanup, event emission, and distributed locking. It also includes methods
    for managing repeatable jobs and handling stalled jobs.
    """

    @abstractmethod
    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> str:
        """
        Asynchronously enqueues a job payload onto the specified queue for
        immediate processing.

        The backend is responsible for storing the job and making it available
        for dequeueing by workers.

        Args:
            queue_name: The name of the queue to add the job to.
            payload: A dictionary containing the job data, including a unique
                     identifier (usually 'id').
        """
        ...

    @abstractmethod
    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously attempts to dequeue a job from the specified queue.

        This method should retrieve a single job that is ready for processing
        and mark it as active or in-progress within the backend. It should
        return None if no jobs are available in the queue.

        Args:
            queue_name: The name of the queue to dequeue from.

        Returns:
            A dictionary representing the job payload if a job was successfully
            dequeued, otherwise None.
        """
        ...

    @abstractmethod
    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        The backend should remove the job from its active state or mark it as
        completed based on this acknowledgment. This is typically called after
        a worker has finished processing a job without errors.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job being acknowledged.
        """
        ...

    @abstractmethod
    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously moves a failed or expired job to the Dead Letter Queue (DLQ).

        This method is called when a job cannot be processed successfully after
        multiple retries or if it has expired. The backend should store the job
        in a designated DLQ or mark it with a status indicating failure.

        Args:
            queue_name: The name of the queue the job originally belonged to.
            payload: A dictionary containing the job data, including its 'id'.
        """
        ...

    @abstractmethod
    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Asynchronously enqueues a job to be processed at a future timestamp.

        The backend is responsible for storing the job and making it available
        for processing only when the specified `run_at` time has been reached
        or passed.

        Args:
            queue_name: The name of the queue the job will eventually be added to.
            payload: A dictionary containing the job data, including an 'id'.
            run_at: A timestamp (float, typically seconds since the epoch)
                    indicating when the job should become eligible for processing.
        """
        ...

    @abstractmethod
    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves delayed jobs from the specified queue that are
        due to run now or in the past.

        The backend should return a list of jobs whose `run_at` timestamp is
        less than or equal to the current time and remove them from the delayed
        storage.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of dictionaries, each representing a job payload that is
            now eligible for processing.
        """
        ...

    @abstractmethod
    async def remove_delayed(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously removes a specific job from the delayed storage.

        This is typically used to cancel a delayed job before its scheduled
        run time.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the delayed job to remove.

        Returns:
            True when a delayed job with `job_id` was found and removed,
            otherwise False.
        """
        ...

    @abstractmethod
    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the processing state of a job.

        This method is used to change the status of a job within the backend's
        storage (e.g., from 'ACTIVE' to 'COMPLETED' or 'FAILED').

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to update.
            state: A string representing the new state of the job
                   (e.g., from `asyncmq.core.enums.State`).
        """
        ...

    @abstractmethod
    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a completed job.

        Stores the output or result data associated with a specific job ID in
        the backend's persistent storage.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job whose result is to be saved.
            result: The result data of the job (can be any serializable type).
        """
        ...

    async def complete_active_job(self, queue_name: str, payload: dict[str, Any], result: Any) -> None:
        """
        Complete an active job as one backend-owned lifecycle transition.

        Storage backends should override this method when they can combine the
        state update, result persistence, and active-job acknowledgement in one
        atomic operation. The default implementation preserves the historical
        multi-call behavior for backends that have not yet been hardened.
        """
        job_id = str(payload["id"])
        await self.update_job_state(queue_name, job_id, State.COMPLETED)
        await self.save_job_result(queue_name, job_id, result)
        await self.ack(queue_name, job_id)

    async def retry_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Move an active job back to delayed retry storage.

        Backends should override this to atomically remove active ownership,
        persist the retried payload, and make the job visible at ``run_at``.
        """
        job_id = str(payload["id"])
        await self.update_job_state(queue_name, job_id, State.DELAYED)
        await self.enqueue_delayed(queue_name, payload, run_at)
        await self.ack(queue_name, job_id)

    async def defer_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Move an active job to delayed storage without changing retry counters.

        This is used for dependency gating and future ``delay_until`` jobs.
        """
        await self.retry_active_job(queue_name, payload, run_at)

    async def fail_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Move an active job to terminal failure and the dead-letter queue.

        Backends should override this when failure state, active ownership, and
        DLQ insertion can be committed atomically.
        """
        job_id = str(payload["id"])
        await self.update_job_state(queue_name, job_id, State.FAILED)
        await self.ack(queue_name, job_id)
        await self.move_to_dlq(queue_name, payload)

    async def expire_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Move an active job to expired terminal state and the dead-letter queue.
        """
        job_id = str(payload["id"])
        await self.update_job_state(queue_name, job_id, State.EXPIRED)
        await self.ack(queue_name, job_id)
        await self.move_to_dlq(queue_name, payload)

    async def cancel_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Release active ownership for a job cancelled before or during execution.
        """
        await self.ack(queue_name, str(payload["id"]))

    async def promote_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Move due delayed jobs into the waiting queue through a backend-owned
        lifecycle transition.

        Concrete backends should override this when their storage can move jobs
        atomically. The default preserves compatibility for minimal backends but
        may remove delayed jobs before enqueueing them.
        """
        jobs = await self.pop_due_delayed(queue_name)
        promoted: list[dict[str, Any]] = []
        for payload in jobs:
            waiting_payload = {**payload, "status": State.WAITING, "delay_until": None}
            await self.enqueue(queue_name, waiting_payload)
            promoted.append(waiting_payload)
        return promoted

    def _prepare_retry_payload(self, payload: dict[str, Any], job_id: str) -> dict[str, Any]:
        retry_payload = {**payload, "id": job_id, "status": State.WAITING, "delay_until": None}
        for key in ("result", "failed_at", "completed_at", "last_error", "error_traceback"):
            retry_payload.pop(key, None)
        return retry_payload

    @abstractmethod
    async def save_job_payload(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Persist a full canonical job payload without changing queue placement.

        AsyncMQ uses this hook for administrative mutations that need to edit
        stored metadata in place, such as clearing a deduplication key or
        extending an existing throttle window. Implementations must update the
        durable payload together with any in-memory mirrors used for dequeueing
        or inspection while preserving the job's current queue membership.

        Args:
            queue_name: The queue that owns the job payload.
            payload: The full canonical job payload. It must include the job
                identifier under ``id`` and should retain the current status.
        """
        ...

    @abstractmethod
    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Asynchronously retrieves the current processing state of a job.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job whose state is requested.

        Returns:
            A string representing the job's state if found, otherwise None.
        """
        ...

    @abstractmethod
    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Asynchronously retrieves the result data of a job.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job whose result is requested.

        Returns:
            The job's result data if the job is found and has a result,
            otherwise None.
        """
        ...

    @abstractmethod
    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Asynchronously registers dependencies for a single job.

        This method is used to link a job to one or more parent jobs that must
        complete successfully before this job can be processed.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The dictionary representing the job, expected to contain
                      a 'depends_on' field which is a list of parent job IDs.
        """
        ...

    @abstractmethod
    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and resolves
        dependencies for any child jobs waiting on it.

        The backend should identify jobs in the specified queue that depend
        on `parent_id` and update their dependency status. If all dependencies
        for a child job are met, it should be made eligible for processing.

        Args:
            queue_name: The name of the queue containing the dependent jobs.
            parent_id: The unique identifier of the parent job that has completed.
        """
        ...

    @abstractmethod
    async def pause_queue(self, queue_name: str) -> None:
        """
        Asynchronously pauses processing on a specific queue.

        Workers should stop dequeueing new jobs from this queue until it is
        resumed. Jobs currently being processed should ideally be allowed to
        finish.

        Args:
            queue_name: The name of the queue to pause.
        """
        ...

    @abstractmethod
    async def resume_queue(self, queue_name: str) -> None:
        """
        Asynchronously resumes processing on a specific queue.

        Allows workers to begin dequeueing jobs from a queue that was previously
        paused.

        Args:
            queue_name: The name of the queue to resume.
        """
        ...

    @abstractmethod
    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Asynchronously checks if a specific queue is currently paused.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the pause state is active for the queue, False otherwise.
        """
        ...

    @abstractmethod
    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a running job.

        Allows workers to report their current progress for a long-running job.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: A float between 0.0 and 1.0 representing the job's progress.
        """
        ...

    @abstractmethod
    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple jobs onto the specified queue in one batch.

        This method is intended for efficiency when adding many jobs at once.

        Args:
            queue_name: The name of the queue to add the jobs to.
            jobs: A list of job dictionaries to enqueue.
        """
        ...

    @abstractmethod
    async def purge(
        self,
        queue_name: str,
        state: str,
        older_than: float | None = None,
    ) -> None:
        """
        Asynchronously removes jobs from the backend based on their state and age.

        This is used for cleanup, removing jobs that are in a specific state
        (e.g., 'COMPLETED', 'FAILED') and optionally older than a given timestamp.

        Args:
            queue_name: The name of the queue to purge jobs from.
            state: The state of the jobs to target for purging.
            older_than: An optional timestamp (float). If provided, only jobs
                        whose relevant timestamp (e.g., completion/failure time)
                        is before this timestamp will be purged. If None, all
                        jobs in the specified state are purged.
        """
        ...

    @abstractmethod
    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Asynchronously emits a backend-specific lifecycle event.

        This allows backends to signal events (e.g., 'job_started', 'job_completed')
        which can be used for monitoring, logging, or UI updates.

        Args:
            event: A string representing the name of the event.
            data: A dictionary containing data associated with the event.
        """
        ...

    @abstractmethod
    async def create_lock(self, key: str, ttl: int) -> Any:
        """
        Asynchronously creates a backend-specific distributed lock.

        This method provides a mechanism for ensuring that only one worker or
        process can acquire a lock for a given key across a distributed system.
        The specific type of the returned lock object depends on the backend
        implementation.

        Args:
            key: A unique string identifier for the lock.
            ttl: The time-to-live (in seconds) for the lock. The lock should
                 automatically expire after this duration if not released.

        Returns:
            An object representing the distributed lock. The exact type depends
            on the backend implementation (e.g., a RedLock instance for Redis).
        """
        ...

    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> list[str]:
        """
        Atomically enqueues multiple jobs and registers their dependencies.

        This method provides a default implementation that sequentially enqueues
        jobs and then adds dependencies. Backends that support true atomic
        operations for flow creation should override this method for better
        consistency and performance.

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
        deps_by_child: dict[str, set[str]] = {}
        for parent, child in dependency_links:
            deps_by_child.setdefault(child, set()).add(parent)

        # Default fallback implementation: enqueue jobs sequentially
        created_ids: list[str] = []
        for jd in job_dicts:
            job_id = jd["id"]
            deps = set(jd.get("depends_on", [])) | deps_by_child.get(job_id, set())
            if deps:
                jd = {**jd, "depends_on": sorted(deps)}
            created_ids.append(job_id)
            await self.enqueue(queue_name, jd)

        # Then register dependencies sequentially
        for child, parents in deps_by_child.items():
            # Add dependency for the child job on the parent job
            await self.add_dependencies(queue_name, {"id": child, "depends_on": sorted(parents)})

        return created_ids  # Return the list of IDs for the enqueued jobs.

    @abstractmethod
    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Asynchronously records the timestamp of the last heartbeat for a running job.

        This is used by the stalled job detection mechanism to track the activity
        of currently processing jobs.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            timestamp: The timestamp (float) of the last heartbeat.
        """
        ...

    @abstractmethod
    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves all jobs whose last heartbeat is older than a
        specified timestamp, indicating they might be stalled.

        Args:
            older_than: A timestamp (float). Jobs whose last heartbeat is before
                        this time are considered potentially stalled.

        Returns:
            A list of dictionaries, where each dictionary represents a potentially
            stalled job and includes information like the queue name and job data.
            The structure might be like `{'queue': queue_name, 'job_data': raw_job}`.
        """
        ...

    @abstractmethod
    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        """
        Asynchronously re-enqueues a stalled job back onto its original queue
        for re-processing.

        This is part of the stalled job recovery process.

        Args:
            queue_name: The name of the queue the stalled job belongs to.
            job_data: The dictionary containing the job data of the stalled job.
        """
        ...

    @abstractmethod
    async def list_delayed(self, queue_name: str) -> list[DelayedInfo]:
        """
        Asynchronously retrieves a list of all currently delayed jobs for a
        specific queue.

        Args:
            queue_name: The name of the queue to list delayed jobs for.

        Returns:
            A list of `DelayedInfo` records.
        """
        ...

    @abstractmethod
    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Asynchronously retrieves a list of all repeatable job definitions for a
        specific queue.

        Args:
            queue_name: The name of the queue to list repeatable jobs for.

        Returns:
            A list of `RepeatableInfo` dataclass instances.
        """
        ...

    @abstractmethod
    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously marks a specific repeatable job definition as paused.

        The scheduler should skip scheduling new instances of a paused repeatable job.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to pause.
        """
        ...

    @abstractmethod
    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Asynchronously un-pauses a repeatable job definition and computes its
        next scheduled run time.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to resume.

        Returns:
            The newly computed timestamp (float) for the next run of the repeatable job.
        """
        ...

    @abstractmethod
    async def cancel_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously cancels a job, removing it from active/waiting/delayed
        queues and marking it so workers will stop processing or skip it.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to cancel.
        """
        ...

    async def upsert_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Create or update a backend-managed repeatable definition.

        Backends that persist schedules should override this method and return
        the next computed run time. The default implementation signals that the
        backend does not support durable repeatable registration.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition.

        Returns:
            The UNIX timestamp for the next scheduled execution.
        """
        raise NotImplementedError("This backend does not support durable repeatable definitions.")

    async def get_due_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Return repeatable definitions whose next run is due.

        The default implementation derives due schedules from ``list_repeatables``.
        Backends can override this to use a more efficient or more atomic query.

        Args:
            queue_name: The queue to inspect.

        Returns:
            The subset of repeatables that are due now and are not paused.
        """
        now = time.time()
        repeatables = await self.list_repeatables(queue_name)
        return [record for record in repeatables if not record.paused and record.next_run <= now]

    async def advance_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Move a repeatable definition forward after one occurrence is enqueued.

        Backends that support durable repeatables should override this method to
        recompute and persist the next run time. The default implementation
        signals that the backend cannot advance repeatable schedules.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition that just fired.

        Returns:
            The newly persisted next-run UNIX timestamp.
        """
        raise NotImplementedError("This backend does not support durable repeatable advancement.")

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any] | str) -> None:
        """
        Remove a backend-managed repeatable definition.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: Either the logical definition or a backend-native identifier.
        """
        raise NotImplementedError("This backend does not support repeatable removal.")

    @abstractmethod
    async def remove_job(self, queue_name: str, job_id: str) -> bool: ...

    @abstractmethod
    async def retry_job(self, queue_name: str, job_id: str) -> bool: ...

    @abstractmethod
    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously checks if a specific job has been marked as cancelled.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to check.

        Returns:
            True if the job is cancelled, False otherwise.
        """
        ...

    @abstractmethod
    async def list_jobs(self, queue: str, state: str) -> list[dict[str, Any]]:
        """List jobs by queue and state."""
        ...

    async def get_job(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        """
        Retrieve the canonical stored payload for a specific job identifier.

        The default implementation performs an inspection-oriented scan across
        all known queue buckets. Backends with native point lookups should
        override this for efficiency, but the fallback preserves a portable
        contract for dashboard and administration tooling.

        Args:
            queue_name: The queue that owns the job.
            job_id: The identifier of the job to retrieve.

        Returns:
            The stored job payload if it can be located, otherwise ``None``.
        """
        for job in await self._inspect_all_jobs(queue_name):
            current_id = job.get("id") or job.get("job_id")
            if current_id is not None and str(current_id) == job_id:
                return dict(job)
        return None

    async def get_jobs(
        self,
        queue_name: str,
        types: list[str] | tuple[str, ...] | None = None,
        *,
        start: int = 0,
        end: int = -1,
        asc: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Return jobs from one or more inspection buckets with pagination.

        This mirrors BullMQ's queue getter surface closely enough for
        operational APIs and dashboards while remaining backend-neutral. Jobs
        are grouped in the order requested by ``types`` and paginated after the
        groups are concatenated.

        Args:
            queue_name: The queue to inspect.
            types: The job types to include. When omitted, the default
                BullMQ-compatible inspection set is used.
            start: Zero-based inclusive start offset.
            end: Zero-based inclusive end offset. ``-1`` means "until the end".
            asc: Whether to keep ascending order within each state bucket.

        Returns:
            The matching jobs after pagination is applied.
        """
        selected_types = sanitize_job_types(types)
        jobs: list[dict[str, Any]] = []
        for job_type in selected_types:
            jobs.extend(await self.list_jobs(queue_name, job_type))
        return paginate_jobs(jobs, start=start, end=end, asc=asc)

    async def get_job_counts(self, queue_name: str, *types: str) -> dict[str, int]:
        """
        Count jobs per inspection bucket for a queue.

        Args:
            queue_name: The queue to inspect.
            *types: Optional job types to count. When omitted, the default
                BullMQ-compatible inspection set is used.

        Returns:
            A mapping of job type to count.
        """
        counts: dict[str, int] = {}
        for job_type in sanitize_job_types(types or None):
            counts[job_type] = len(await self.list_jobs(queue_name, job_type))
        return counts

    async def get_job_count_by_types(self, queue_name: str, *types: str) -> int:
        """
        Return the total number of jobs across one or more inspection buckets.

        Args:
            queue_name: The queue to inspect.
            *types: Optional job types to include in the total.

        Returns:
            The summed job count.
        """
        counts = await self.get_job_counts(queue_name, *types)
        return sum(counts.values())

    async def count(self, queue_name: str) -> int:
        """
        Return the number of jobs waiting to be processed.

        In BullMQ this total includes waiting, delayed, prioritized, paused,
        and waiting-children buckets. AsyncMQ does not materialize distinct
        paused or prioritized buckets, so their contribution is zero unless a
        backend implements them explicitly.

        Args:
            queue_name: The queue to inspect.

        Returns:
            The total queued workload count.
        """
        return await self.get_job_count_by_types(
            queue_name,
            "waiting",
            "paused",
            "delayed",
            "prioritized",
            "waiting-children",
        )

    async def drain_queue(self, queue_name: str, *, include_delayed: bool = False) -> list[str]:
        """
        Remove jobs that are still queued and have not started executing.

        Args:
            queue_name: The queue to drain.
            include_delayed: Whether delayed jobs should also be removed.

        Returns:
            The identifiers of the removed jobs.
        """
        states = ["waiting"]
        if include_delayed:
            states.append("delayed")

        removed_ids: list[str] = []
        for state in states:
            for job in await self.list_jobs(queue_name, state):
                job_id = job.get("id") or job.get("job_id")
                if job_id is None:
                    continue
                if await self.remove_job(queue_name, str(job_id)):
                    removed_ids.append(str(job_id))
        return removed_ids

    async def clean_jobs(
        self,
        queue_name: str,
        *,
        state: str = "completed",
        grace: float = 0,
        limit: int = 0,
    ) -> list[str]:
        """
        Remove jobs from a queue based on their inspection bucket and age.

        Args:
            queue_name: The queue to clean.
            state: The job bucket to clean.
            grace: The minimum age in seconds a job must have before removal.
            limit: The maximum number of jobs to delete. ``0`` means no limit.

        Returns:
            The identifiers of the removed jobs.
        """
        deadline = time.time() - grace
        removed_ids: list[str] = []
        for job in await self.list_jobs(queue_name, state):
            if limit and len(removed_ids) >= limit:
                break

            reference_ts = self._job_cleanup_timestamp(job, state)
            if reference_ts > deadline:
                continue

            job_id = job.get("id") or job.get("job_id")
            if job_id is None:
                continue
            if await self.remove_job(queue_name, str(job_id)):
                removed_ids.append(str(job_id))
        return removed_ids

    async def obliterate_queue(self, queue_name: str, *, force: bool = False) -> list[str]:
        """
        Irreversibly remove all jobs and repeatable definitions for a queue.

        Args:
            queue_name: The queue to obliterate.
            force: When ``False``, active jobs block the operation.

        Returns:
            The identifiers of removed jobs.

        Raises:
            RuntimeError: If active jobs exist and ``force`` is not enabled.
        """
        active = await self.list_jobs(queue_name, "active")
        if active and not force:
            raise RuntimeError("Cannot obliterate a queue that still has active jobs. Pass force=True to override.")

        removed_ids: list[str] = []
        for job in await self._inspect_all_jobs(queue_name):
            job_id = job.get("id") or job.get("job_id")
            if job_id is None:
                continue
            if await self.remove_job(queue_name, str(job_id)):
                removed_ids.append(str(job_id))

        remove_repeatable = getattr(self, "remove_repeatable", None)
        if callable(remove_repeatable):
            for record in await self.list_repeatables(queue_name):
                await remove_repeatable(queue_name, record.job_def)

        return removed_ids

    @abstractmethod
    async def queue_stats(self, queue_name: str) -> dict[str, int]: ...

    @abstractmethod
    async def list_queues(self) -> list[str]: ...

    @abstractmethod
    async def register_worker(self, worker_id: str, queue: str, concurrency: int, timestamp: float) -> None:
        """Register or update a worker's heartbeat and metadata."""
        ...

    @abstractmethod
    async def deregister_worker(self, worker_id: str) -> None:
        """Remove a worker explicitly (on clean shutdown)."""
        ...

    @abstractmethod
    async def list_workers(self) -> list[WorkerInfo]:
        """Return all workers with heartbeat ≥ now - monkay.settings.heartbeat_ttl."""
        ...

    @abstractmethod
    async def pop_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Atomically fetches *and* removes all delayed jobs whose run_at ≤ now.
        Returns the list of job-dicts.
        """
        ...

    @property
    def _settings(self) -> Settings:
        return asyncmq.monkay.settings

    @property
    def _json_serializer(self) -> JSONSerializer:
        return self._settings.json_serializer

    async def _inspect_all_jobs(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Collect a de-duplicated set of jobs across all portable inspection buckets.

        This helper powers backend-neutral queue getters and admin operations.
        The result is intentionally conservative: unsupported buckets simply
        contribute no jobs.

        Args:
            queue_name: The queue to inspect.

        Returns:
            The de-duplicated stored job payloads.
        """
        states = (
            "waiting",
            "waiting-children",
            "active",
            "completed",
            "failed",
            "delayed",
            "expired",
            "queued",
        )
        seen: dict[str, dict[str, Any]] = {}
        for state in states:
            for job in await self.list_jobs(queue_name, state):
                job_id = job.get("id") or job.get("job_id")
                if job_id is None:
                    continue
                seen[str(job_id)] = dict(job)
        return list(seen.values())

    def _job_cleanup_timestamp(self, job: dict[str, Any], state: str) -> float:
        """
        Choose the timestamp used when evaluating whether a job is old enough
        for a cleanup operation.

        Args:
            job: The stored job payload.
            state: The cleanup bucket requested by the caller.

        Returns:
            The timestamp that should be compared against the cleanup grace
            deadline.
        """
        if state == "completed":
            return float(job.get("completed_at") or job.get("updated_at") or job.get("created_at") or 0.0)
        if state in {"failed", "expired"}:
            return float(job.get("failed_at") or job.get("updated_at") or job.get("created_at") or 0.0)
        if state == "delayed":
            return float(job.get("delay_until") or job.get("updated_at") or job.get("created_at") or 0.0)
        return float(job.get("updated_at") or job.get("created_at") or 0.0)
