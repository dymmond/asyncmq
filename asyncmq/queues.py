import time
from typing import Any, cast

import anyio

import asyncmq
from asyncmq.backends.base import BaseBackend, DelayedInfo, RepeatableInfo
from asyncmq.core.deduplication import (
    can_replace_deduplicated_job,
    normalize_deduplication_options,
    select_deduplication_owner,
    with_cleared_deduplication,
    with_updated_deduplication_window,
)
from asyncmq.core.inspection import infer_job_type
from asyncmq.core.job_ids import validate_custom_job_id
from asyncmq.core.locks import acquire_backend_lock, release_backend_lock
from asyncmq.jobs import Job
from asyncmq.runners import run_worker


class Queue:
    """
    A high-level API for managing and interacting with a message queue.

    This class provides methods for enqueuing jobs, scheduling repeatable tasks,
    controlling worker behavior (pause/resume), cleaning up jobs, and running
    a worker process to consume jobs from the queue. It acts as the primary
    interface for users to interact with the asyncmq system.

    Key Features:
    - Add single jobs (`add`) or multiple jobs in bulk (`add_bulk`).
    - Schedule jobs to repeat at regular intervals (`add_repeatable`).
    - Control queue processing state (`pause`, `resume`).
    - Clean up jobs based on state and age (`clean`).
    - Start a worker process to consume jobs from this queue (`run`, `start`).
    """

    def __init__(
        self,
        name: str,
        backend: BaseBackend | None = None,
        concurrency: int = 3,
        rate_limit: int | None = None,
        rate_interval: float = 1.0,
        scan_interval: float | None = None,
    ) -> None:
        """
        Intializes a Queue instance.

        Args:
            name: The unique name of the queue. Jobs and workers are associated
                  with a specific queue name.
            backend: An optional backend instance to use for queue storage and
                     operations. If None, a `RedisBackend` instance is created
                     and used by default.
            concurrency: The maximum number of jobs that workers processing this
                         queue are allowed to handle concurrently. Defaults to 3.
            rate_limit: Configures rate limiting for workers processing this queue.
                        - If None (default), rate limiting is disabled.
                        - If an integer > 0, workers will process a maximum of
                          `rate_limit` jobs per `rate_interval`.
                        - If 0, job processing is effectively blocked.
            rate_interval: The time window in seconds over which the `rate_limit`
                           applies. Defaults to 1.0 second.
            scan_interval: How often (seconds) to poll delayed and repeatable jobs.
                           Overrides global `monkay.settings.scan_interval` if provided.
                           Defaults to `monkay.settings.scan_interval`.
        """
        self.name: str = name
        self._settings = asyncmq.monkay.settings
        # Use the provided backend or fall back to the default configured backend.
        self.backend: BaseBackend = backend or self._settings.backend
        # Internal list to store configurations for repeatable jobs.
        self._repeatables: list[dict[str, Any]] = []
        self.concurrency: int = concurrency
        self.rate_limit: int | None = rate_limit
        self.rate_interval: float = rate_interval
        self.scan_interval: float = scan_interval if scan_interval is not None else self._settings.scan_interval

    def _job_id_lock_key(self, job_id: str | None) -> str | None:
        """
        Return the backend lock key used to serialize custom job-id checks.

        Args:
            job_id: The optional custom job identifier requested by the producer.

        Returns:
            A backend lock key when a custom identifier is present, otherwise
            ``None``.
        """
        if job_id is None:
            return None
        return f"asyncmq:{self.name}:job-id:{job_id}"

    def _deduplication_lock_key(self, deduplication_id: str | None) -> str | None:
        """
        Return the backend lock key used to serialize deduplicated producers.

        Args:
            deduplication_id: The logical deduplication identifier.

        Returns:
            A backend lock key when deduplication is enabled, otherwise
            ``None``.
        """
        if deduplication_id is None:
            return None
        return f"asyncmq:{self.name}:dedup:{deduplication_id}"

    async def _enqueue_created_job(self, job: Job, *, delay: float | None) -> None:
        """
        Persist a freshly created job using the appropriate queue bucket.

        Args:
            job: The job instance to persist.
            delay: Optional delay in seconds before the job becomes available.
        """
        if delay is not None:
            job.delay_until = time.time() + delay
            await self.backend.enqueue_delayed(self.name, job.to_dict(), job.delay_until)
            return
        await self.backend.enqueue(self.name, job.to_dict())

    async def _find_deduplication_owner(self, deduplication_id: str) -> dict[str, Any] | None:
        """
        Inspect the queue for the current owner of a deduplication key.

        Args:
            deduplication_id: The logical deduplication identifier.

        Returns:
            The canonical job payload that currently owns the deduplication
            window, or ``None`` when the key is inactive.
        """
        jobs = await self.backend._inspect_all_jobs(self.name)
        return select_deduplication_owner(jobs, deduplication_id)

    async def add(
        self,
        task_id: str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        retries: int = 0,
        ttl: int | None = None,
        backoff: float | None = None,
        priority: int = 5,
        delay: float | None = None,
        job_id: str | None = None,
        deduplication: dict[str, Any] | None = None,
        debounce: dict[str, Any] | None = None,
    ) -> str:
        """
        Creates and enqueues a single job onto this queue.

        The job is scheduled for immediate processing unless a `delay` is specified.

        Args:
            task_id: The unique identifier string for the task function that this
                     job should execute. This ID should correspond to a function
                     registered with the `@task` decorator.
            args: An optional list of positional arguments to pass to the task
                  function when the job is executed. Defaults to an empty list.
            kwargs: An optional dictionary of keyword arguments to pass to the
                    task function. Defaults to an empty dictionary.
            retries: The maximum number of times this job should be retried
                     in case of failure. Defaults to 0 (no retries).
            ttl: The time-to-live (TTL) for this job in seconds. If the job is
                 not processed within this time, it expires. Defaults to None.
            backoff: A factor used in calculating the delay between retry attempts
                     (e.g., for exponential backoff). Defaults to None.
            priority: The priority level of the job. Lower numbers indicate higher
                      priority. Defaults to 5.
            delay: If set to a non-negative float, the job will be scheduled to
                   be available for processing after this many seconds from the
                   current time. If None, the job is enqueued immediately.
                   Defaults to None.
            job_id: An optional custom job identifier used to make producer
                    requests idempotent. When a non-removed job with the same
                    identifier already exists in this queue, AsyncMQ returns the
                    existing identifier and skips creating a duplicate job.
            deduplication: Optional BullMQ-style deduplication settings. AsyncMQ
                           supports simple mode (no TTL), throttle mode (TTL),
                           and debounce/replace mode for delayed jobs.
            debounce: Deprecated BullMQ alias for ``deduplication``. It is
                      accepted for migration compatibility.

        Returns:
            The unique ID string assigned to the newly created job.
        """
        if job_id is not None:
            validate_custom_job_id(job_id)

        normalized_deduplication = normalize_deduplication_options(
            deduplication,
            debounce,
            delay=delay,
        )
        job = Job(
            task_id=task_id,
            args=args or [],
            kwargs=kwargs or {},
            retries=0,
            max_retries=retries,
            backoff=backoff,
            ttl=ttl,
            priority=priority,
            job_id=job_id,
            deduplication=normalized_deduplication,
        )
        lock = None
        lock_key = self._deduplication_lock_key(
            normalized_deduplication["id"] if normalized_deduplication is not None else None
        ) or self._job_id_lock_key(job_id)

        if lock_key is not None:
            lock = await self.backend.create_lock(lock_key, ttl=30)
            await acquire_backend_lock(lock)

        try:
            if job_id is not None and await self.get_job(job.id) is not None:
                await self.backend.emit_event("job:duplicated", {"queue": self.name, "job_id": job.id})
                return job.id

            if normalized_deduplication is not None:
                deduplication_id = str(normalized_deduplication["id"])
                owner = await self._find_deduplication_owner(deduplication_id)
                if owner is not None:
                    owner_id = str(owner["id"])
                    ttl_window = normalized_deduplication.get("ttl")

                    if normalized_deduplication.get("replace") and can_replace_deduplicated_job(owner):
                        removed = await self.backend.remove_job(self.name, owner_id)
                        if removed:
                            if ttl_window is not None and not normalized_deduplication.get("extend"):
                                existing = owner.get("deduplication", {})
                                if isinstance(existing.get("expires_at"), (int, float)):
                                    normalized_deduplication["expires_at"] = float(existing["expires_at"])
                            elif ttl_window is not None:
                                normalized_deduplication["expires_at"] = time.time() + float(ttl_window)
                            job.deduplication = normalized_deduplication
                            await self._enqueue_created_job(job, delay=delay)
                            await self.backend.emit_event(
                                "job:debounced",
                                {"queue": self.name, "job_id": job.id, "debounce_id": deduplication_id},
                            )
                            await self.backend.emit_event(
                                "job:deduplicated",
                                {
                                    "queue": self.name,
                                    "job_id": job.id,
                                    "deduplication_id": deduplication_id,
                                    "deduplicated_job_id": owner_id,
                                },
                            )
                            return job.id

                    if ttl_window is not None and normalized_deduplication.get("extend"):
                        updated_owner = with_updated_deduplication_window(
                            owner,
                            ttl=float(ttl_window),
                            keep_existing_expiry=False,
                        )
                        await self.backend.save_job_payload(self.name, updated_owner)
                        owner = updated_owner
                        owner_id = str(owner["id"])

                    await self.backend.emit_event(
                        "job:debounced",
                        {"queue": self.name, "job_id": owner_id, "debounce_id": deduplication_id},
                    )
                    await self.backend.emit_event(
                        "job:deduplicated",
                        {
                            "queue": self.name,
                            "job_id": owner_id,
                            "deduplication_id": deduplication_id,
                            "deduplicated_job_id": job.id,
                        },
                    )
                    return owner_id

            await self._enqueue_created_job(job, delay=delay)
            return job.id
        finally:
            if lock is not None:
                await release_backend_lock(lock)

    async def add_bulk(self, jobs: list[dict[str, Any]]) -> list[str]:
        """
        Creates and enqueues multiple jobs onto this queue while preserving
        single-job semantics.

        AsyncMQ intentionally routes each job through `Queue.add(...)` so
        custom job identifiers, deduplication windows, and delayed-job replace
        rules behave the same whether producers enqueue one job or many.

        Args:
            jobs: A list of dictionaries, where each dictionary specifies the
                  configuration for a job to be created and enqueued. Expected
                  keys mirror `Job` constructor parameters (e.g., "task_id",
                  "args", "kwargs", "retries", "ttl", "priority").

        Returns:
            A list of unique ID strings for the newly created jobs, in the
            same order as the input list of job configurations.
        """
        created_ids: list[str] = []
        for cfg in jobs:
            task_id = cfg.get("task_id")
            if not isinstance(task_id, str):
                raise ValueError("Each job config must include a string 'task_id'.")
            requested_job_id = cfg.get("job_id") or cfg.get("jobId")
            if requested_job_id is not None and not isinstance(requested_job_id, str):
                raise TypeError("Bulk job 'job_id' values must be strings when provided.")
            created_ids.append(
                await self.add(
                    task_id=task_id,
                    args=cfg.get("args"),
                    kwargs=cfg.get("kwargs"),
                    retries=cfg.get("retries", 0),
                    ttl=cfg.get("ttl"),
                    backoff=cfg.get("backoff"),
                    priority=cfg.get("priority", 5),
                    delay=cfg.get("delay"),
                    job_id=requested_job_id,
                    deduplication=cfg.get("deduplication"),
                    debounce=cfg.get("debounce"),
                )
            )

        return created_ids

    def add_repeatable(
        self,
        task_id: str,
        every: float | str | None = None,
        cron: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        retries: int = 0,
        ttl: int | None = None,
        priority: int = 5,
        backoff: float | int | None = None,
    ) -> None:
        """
        Registers a job definition to be scheduled and enqueued repeatedly
        by the worker's internal scheduler.

        This method does *not* immediately enqueue a job. Instead, it adds
        the job definition to an internal list (`_repeatables`). When the
        `run()` or `start()` method is called to start the worker, a separate
        scheduler task is launched which periodically checks these registered
        definitions and enqueues new jobs based on the `every` interval or
        `cron` expression.

        Args:
            task_id: The unique identifier string for the task function to
                     execute for repeatable jobs.
            every: The time interval in seconds between each repeatable job
                   instance being enqueued (e.g., 60.0 for every minute) OR
                   a string recognizable by the scheduler for interval definition.
                   Defaults to None.
            cron: A cron expression string defining the schedule for repeatable
                  jobs (e.g., "0 * * * *" for hourly). Defaults to None.
            args: An optional list of positional arguments to pass to the task
                  function for each repeatable job instance. Defaults to [].
            kwargs: An optional dictionary of keyword arguments to pass to the
                    task function. Defaults to {}.
            retries: The maximum number of retries for each instance of the
                  repeatable job if it fails. Defaults to 0.
            ttl: The TTL in seconds for each instance of the repeatable job.
                 Defaults to None.
            priority: The priority for each instance of the repeatable job.
                      Defaults to 5.
            backoff: The numeric backoff value applied to retries of each
                     generated job instance. Defaults to None.

        Raises:
            ValueError: If neither `every` nor `cron` is provided.
        """
        # Validate that either 'every' or 'cron' is provided.
        if not every and not cron:
            raise ValueError("Either 'every' (seconds or string) or 'cron' (expression) must be provided.")

        # Create a dictionary representing the repeatable job entry.
        entry: dict[str, Any] = {
            "task_id": task_id,
            "args": args or [],
            "kwargs": kwargs or {},
            "retries": retries,
            "ttl": ttl,
            "priority": priority,
        }
        if backoff is not None:
            entry["backoff"] = backoff
        # Add 'every' or 'cron' to the entry if provided.
        if every:
            entry["every"] = every
        if cron:
            entry["cron"] = cron

        # Append the repeatable job entry to the internal list.
        self._repeatables.append(entry)

    async def upsert_repeatable(
        self,
        task_id: str,
        every: float | str | None = None,
        cron: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        retries: int = 0,
        ttl: int | None = None,
        priority: int = 5,
        backoff: float | int | None = None,
    ) -> float:
        """
        Create or update a backend-managed repeatable definition.

        Unlike ``add_repeatable``, this API persists the schedule in the
        backend so workers can discover it without inheriting in-process state
        from the producer process. This is the recommended API for durable
        scheduling and dashboard-driven repeatable management.

        Args:
            task_id: The registered task identifier to execute.
            every: A fixed interval in seconds between occurrences.
            cron: A cron expression controlling the schedule.
            args: Positional arguments passed to each generated job.
            kwargs: Keyword arguments passed to each generated job.
            retries: The maximum retries allowed for each generated job.
            ttl: Optional TTL for each generated job.
            priority: Priority assigned to each generated job instance.
            backoff: The numeric backoff value applied to retries of each
                     generated job instance.

        Returns:
            The UNIX timestamp for the next scheduled execution.

        Raises:
            ValueError: If neither ``every`` nor ``cron`` is provided.
            NotImplementedError: If the configured backend does not support
                backend-managed repeatables.
        """
        if not every and not cron:
            raise ValueError("Either 'every' (seconds or string) or 'cron' (expression) must be provided.")

        job_def: dict[str, Any] = {
            "task_id": task_id,
            "args": args or [],
            "kwargs": kwargs or {},
            "retries": retries,
            "ttl": ttl,
            "priority": priority,
        }
        if backoff is not None:
            job_def["backoff"] = backoff
        if every:
            job_def["every"] = every
        if cron:
            job_def["cron"] = cron

        upsert_repeatable = getattr(self.backend, "upsert_repeatable", None)
        if not callable(upsert_repeatable):
            raise NotImplementedError("This backend does not support backend-managed repeatables.")

        return await cast(Any, upsert_repeatable)(self.name, job_def)

    async def pause(self) -> None:
        """
        Signals the backend to pause job processing for this specific queue.

        Workers consuming from a paused queue should stop dequeueing new jobs
        until the queue is resumed. Jobs currently being processed might finish
        depending on the backend implementation and worker logic.
        """
        # Instruct the backend to pause this queue.
        await self.backend.pause_queue(self.name)

    async def resume(self) -> None:
        """
        Signals the backend to resume job processing for this specific queue.

        If the queue was previously paused, workers will begin dequeueing and
        processing jobs again after this method is called.
        """
        # Instruct the backend to resume this queue.
        await self.backend.resume_queue(self.name)

    async def clean(
        self,
        state: str,
        older_than: float | None = None,
    ) -> None:
        """
        Requests the backend to purge jobs from this queue based on their state
        and age.

        Args:
            state: The state of the jobs to be purged (e.g., "completed",
                   "failed", "expired"). The exact states supported depend
                   on the backend implementation.
            older_than: An optional timestamp (as a float, e.g., from time.time()).
                        Only jobs in the specified `state` whose processing
                        timestamp (completion, failure, expiration time) is
                        older than this value will be purged. If None, all
                        jobs in the specified state are potentially purged.
        """
        # Instruct the backend to purge jobs from this queue based on state and age.
        await self.backend.purge(self.name, state, older_than)

    async def clean_jobs(
        self,
        grace: float,
        limit: int,
        state: str = "completed",
    ) -> list[str]:
        """
        Remove jobs using BullMQ-style cleanup semantics.

        Unlike :meth:`clean`, which preserves AsyncMQ's older queue-local purge
        API, this method matches BullMQ's "grace plus limit" model closely and
        returns the identifiers of removed jobs for administrative tooling.

        Args:
            grace: The minimum age in seconds a job must have before removal.
            limit: The maximum number of jobs to remove. ``0`` means no limit.
            state: The inspection bucket to clean.

        Returns:
            The identifiers of removed jobs.
        """
        return await self.backend.clean_jobs(self.name, state=state, grace=grace, limit=limit)

    async def run(self) -> None:
        """
        Starts the asynchronous worker process for this queue.

        This method launches the core worker tasks, including the main job
        processor, the delayed job scanner, and potentially a repeatable
        job scheduler (if repeatable tasks were added). The worker runs with
        the concurrency and rate limit settings configured during Queue
        initialization. This is an asynchronous function and will run until
        cancelled.
        """
        # Start the worker process with configured parameters.
        await run_worker(
            self.name,
            self.backend,
            concurrency=self.concurrency,
            rate_limit=self.rate_limit,
            rate_interval=self.rate_interval,
            scan_interval=self.scan_interval,
            repeatables=self._repeatables,
        )

    def start(self) -> None:
        """
        Provides a synchronous entry point to start the queue worker.

        This method is a convenience wrapper that calls the asynchronous `run()`
        method using `anyio.run()`. It is typically used when starting the worker
        from a non-asynchronous context (e.g., a standard script or application
        entry point). This call is blocking and will not return until the
        worker's `run()` method completes (which usually happens when the worker
        task is cancelled).
        """
        # Run the asynchronous 'run' method within an AnyIO event loop.
        anyio.run(self.run)

    async def enqueue(self, payload: dict[str, Any]) -> str:
        """
        Enqueue a job for immediate processing.
        """
        job_id = payload.get("id")
        if job_id is None:
            # Create a Job to mint an id and normalize the payload.
            job = Job(
                task_id=payload["task_id"],
                args=payload.get("args", []),
                kwargs=payload.get("kwargs", {}),
                retries=0,
                max_retries=payload.get("retries", 0),
                backoff=payload.get("backoff"),
                ttl=payload.get("ttl"),
                priority=payload.get("priority", 5),
            )
            payload = job.to_dict()
            job_id = job.id

        await self.backend.enqueue(self.name, payload)
        return cast(str, job_id)

    async def enqueue_delayed(self, payload: dict[str, Any], run_at: float) -> str:
        """
        Schedule a job to run at a future UNIX timestamp.
        """
        job_id = payload.get("id")
        if job_id is None:
            job = Job(
                task_id=payload["task_id"],
                args=payload.get("args", []),
                kwargs=payload.get("kwargs", {}),
                retries=0,
                max_retries=payload.get("retries", 0),
                backoff=payload.get("backoff"),
                ttl=payload.get("ttl"),
                priority=payload.get("priority", 5),
            )
            job.delay_until = run_at
            payload = job.to_dict()
            job_id = job.id
        else:
            # Ensure the run_at is present if caller minted their own id/payload
            payload["delay_until"] = run_at

        await self.backend.enqueue_delayed(self.name, payload, run_at)
        return cast(str, job_id)

    async def delay(self, payload: dict[str, Any], run_at: float | None = None) -> str:
        """
        The same of enqueue with enqueue_delayed combined in one place.
        """
        if run_at is None:
            return await self.enqueue(payload)
        return await self.enqueue_delayed(payload, run_at)

    async def send(self, payload: dict[str, Any]) -> str:
        """
        The same as enqueue but under a different interface name.
        """
        return await self.enqueue(payload)

    async def get_due_delayed(self) -> list[dict[str, Any]]:
        """
        Pop & return any jobs whose run_at ≤ now.
        """
        return await self.backend.get_due_delayed(self.name)

    async def list_delayed(self) -> list[DelayedInfo]:
        return await self.backend.list_delayed(self.name)

    async def remove_delayed(self, job_id: str) -> bool:
        return await self.backend.remove_delayed(self.name, job_id)  # type: ignore

    async def list_repeatables(self) -> list[RepeatableInfo]:
        return await self.backend.list_repeatables(self.name)

    async def remove_repeatable(self, job_def: dict[str, Any]) -> None:
        remove_repeatable = getattr(self.backend, "remove_repeatable", None)
        if not callable(remove_repeatable):
            raise NotImplementedError("This backend does not support repeatable removal.")
        await cast(Any, remove_repeatable)(self.name, job_def)

    async def pause_repeatable(self, job_def: dict[str, Any]) -> None:
        await self.backend.pause_repeatable(self.name, job_def)

    async def resume_repeatable(self, job_def: dict[str, Any]) -> float:
        return await self.backend.resume_repeatable(self.name, job_def)

    async def cancel_job(self, job_id: str) -> bool:
        return await self.backend.cancel_job(self.name, job_id)

    async def is_job_cancelled(self, job_id: str) -> bool:
        return await self.backend.is_job_cancelled(self.name, job_id)

    async def queue_stats(self) -> dict[str, int]:
        """
        Get counts of waiting, delayed, failed for this queue.
        """
        return await self.backend.queue_stats(self.name)

    async def list_jobs(self, state: str) -> list[dict[str, Any]]:
        """
        List jobs that belong to a single inspection bucket.

        Args:
            state: The state or inspection bucket to query.

        Returns:
            The matching job payloads in backend-defined order.
        """
        return await self.backend.list_jobs(self.name, state)

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        """
        Retrieve a single job payload by identifier.

        Args:
            job_id: The identifier of the job to inspect.

        Returns:
            The stored job payload if found, otherwise ``None``.
        """
        return await self.backend.get_job(self.name, job_id)

    async def get_job_state(self, job_id: str) -> str | None:
        """
        Retrieve the public inspection state for a job.

        This method prefers the richer inspection view over the backend's raw
        runtime state so unresolved dependency jobs appear as
        ``waiting-children`` when queried from queue administration surfaces.

        Args:
            job_id: The identifier of the job to inspect.

        Returns:
            The inferred inspection state, or ``None`` when the job does not exist.
        """
        job = await self.get_job(job_id)
        if job is None:
            return None
        return infer_job_type(job)

    async def get_job_result(self, job_id: str) -> Any | None:
        """
        Retrieve the stored result for a job.

        Args:
            job_id: The identifier of the job to inspect.

        Returns:
            The stored job result, or ``None`` if no result is available.
        """
        return await self.backend.get_job_result(self.name, job_id)

    async def get_deduplication_job_id(self, deduplication_id: str) -> str | None:
        """
        Return the job id that currently owns a deduplication window.

        Args:
            deduplication_id: The logical deduplication identifier.

        Returns:
            The owning job id when the deduplication key is active, otherwise
            ``None``.
        """
        owner = await self._find_deduplication_owner(deduplication_id)
        if owner is None:
            return None
        return str(owner["id"])

    async def get_debounce_job_id(self, deduplication_id: str) -> str | None:
        """
        Backwards-compatible alias for `get_deduplication_job_id`.

        Args:
            deduplication_id: The legacy debounce identifier.

        Returns:
            The owning job id when active, otherwise ``None``.
        """
        return await self.get_deduplication_job_id(deduplication_id)

    async def remove_deduplication_key(self, deduplication_id: str) -> bool:
        """
        Remove a deduplication window before it expires naturally.

        AsyncMQ implements this by clearing deduplication metadata from any
        stored job payloads that currently own the key. This preserves the job
        itself while allowing later producer calls to enqueue a new copy.

        Args:
            deduplication_id: The logical deduplication identifier.

        Returns:
            ``True`` if at least one job payload was updated, otherwise
            ``False``.
        """
        changed = False
        for job in await self.backend._inspect_all_jobs(self.name):
            deduplication = job.get("deduplication")
            if not isinstance(deduplication, dict) or deduplication.get("id") != deduplication_id:
                continue
            await self.backend.save_job_payload(self.name, with_cleared_deduplication(job))
            changed = True
        return changed

    async def remove_job(self, job_id: str) -> bool:
        """
        Remove a specific job from this queue.

        Args:
            job_id: The identifier of the job to remove.

        Returns:
            ``True`` when a job was removed, otherwise ``False``.
        """
        return await self.backend.remove_job(self.name, job_id)

    async def retry_job(self, job_id: str) -> bool:
        """
        Retry a previously failed or delayed job.

        Args:
            job_id: The identifier of the job to retry.

        Returns:
            ``True`` when a retry was scheduled, otherwise ``False``.
        """
        return await self.backend.retry_job(self.name, job_id)

    async def get_jobs(
        self,
        types: list[str] | tuple[str, ...] | None = None,
        *,
        start: int = 0,
        end: int = -1,
        asc: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Retrieve jobs from one or more inspection buckets with pagination.

        Args:
            types: The inspection buckets to include. When omitted, AsyncMQ
                uses the default BullMQ-compatible set.
            start: Zero-based inclusive start offset.
            end: Zero-based inclusive end offset. ``-1`` means "until the end".
            asc: Whether to keep ascending order within each requested bucket.

        Returns:
            The matching job payloads.
        """
        return await self.backend.get_jobs(self.name, types, start=start, end=end, asc=asc)

    async def get_job_counts(self, *types: str) -> dict[str, int]:
        """
        Count jobs per inspection bucket for this queue.

        Args:
            *types: Optional inspection buckets. When omitted, the default
                BullMQ-compatible set is used.

        Returns:
            A mapping of inspection bucket to count.
        """
        return await self.backend.get_job_counts(self.name, *types)

    async def get_job_count_by_types(self, *types: str) -> int:
        """
        Count jobs across one or more inspection buckets.

        Args:
            *types: Optional inspection buckets.

        Returns:
            The total number of matching jobs.
        """
        return await self.backend.get_job_count_by_types(self.name, *types)

    async def count(self) -> int:
        """
        Return the number of jobs still queued for future processing.

        Returns:
            The combined workload count across waiting-oriented buckets.
        """
        return await self.backend.count(self.name)

    async def get_waiting(self, *, start: int = 0, end: int = -1, asc: bool = False) -> list[dict[str, Any]]:
        """Return waiting jobs for this queue."""
        return await self.get_jobs(["waiting"], start=start, end=end, asc=asc)

    async def get_active(self, *, start: int = 0, end: int = -1, asc: bool = False) -> list[dict[str, Any]]:
        """Return active jobs for this queue."""
        return await self.get_jobs(["active"], start=start, end=end, asc=asc)

    async def get_completed(self, *, start: int = 0, end: int = -1, asc: bool = False) -> list[dict[str, Any]]:
        """Return completed jobs for this queue."""
        return await self.get_jobs(["completed"], start=start, end=end, asc=asc)

    async def get_failed(self, *, start: int = 0, end: int = -1, asc: bool = False) -> list[dict[str, Any]]:
        """Return failed jobs for this queue."""
        return await self.get_jobs(["failed"], start=start, end=end, asc=asc)

    async def get_delayed(self, *, start: int = 0, end: int = -1, asc: bool = False) -> list[dict[str, Any]]:
        """Return delayed jobs for this queue."""
        return await self.get_jobs(["delayed"], start=start, end=end, asc=asc)

    async def get_waiting_children(
        self,
        *,
        start: int = 0,
        end: int = -1,
        asc: bool = False,
    ) -> list[dict[str, Any]]:
        """Return jobs waiting on unfinished parent dependencies."""
        return await self.get_jobs(["waiting-children"], start=start, end=end, asc=asc)

    async def get_paused(self, *, start: int = 0, end: int = -1, asc: bool = False) -> list[dict[str, Any]]:
        """Return jobs in the paused inspection bucket."""
        return await self.get_jobs(["paused"], start=start, end=end, asc=asc)

    async def get_prioritized(
        self,
        *,
        start: int = 0,
        end: int = -1,
        asc: bool = False,
    ) -> list[dict[str, Any]]:
        """Return jobs in the prioritized inspection bucket."""
        return await self.get_jobs(["prioritized"], start=start, end=end, asc=asc)

    async def drain(self, include_delayed: bool = False) -> list[str]:
        """
        Remove queued jobs that have not started execution yet.

        Args:
            include_delayed: Whether delayed jobs should also be removed.

        Returns:
            The identifiers of removed jobs.
        """
        return await self.backend.drain_queue(self.name, include_delayed=include_delayed)

    async def obliterate(self, *, force: bool = False) -> list[str]:
        """
        Irreversibly remove all jobs and repeatable definitions for this queue.

        Args:
            force: Whether active jobs should be ignored.

        Returns:
            The identifiers of removed jobs.
        """
        return await self.backend.obliterate_queue(self.name, force=force)
