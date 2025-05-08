import time
from typing import Any

import anyio

from asyncmq.backends.base import BaseBackend, RepeatableInfo
from asyncmq.conf import monkay
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
        # Use the provided backend or fall back to the default configured backend.
        self.backend: BaseBackend = backend or monkay.settings.backend
        # Internal list to store configurations for repeatable jobs.
        self._repeatables: list[dict[str, Any]] = []
        self.concurrency: int = concurrency
        self.rate_limit: int | None = rate_limit
        self.rate_interval: float = rate_interval
        self.scan_interval: float = scan_interval or monkay.settings.scan_interval

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

        Returns:
            The unique ID string assigned to the newly created job.
        """
        # Create a Job object from the provided arguments and configuration.
        job = Job(
            task_id=task_id,
            args=args or [],
            kwargs=kwargs or {},
            retries=0,
            max_retries=retries,
            backoff=backoff,
            ttl=ttl,
            priority=priority,
        )
        # Enqueue the job, either with a delay or immediately, using the backend.
        if delay is not None:
            job.delay_until = time.time() + delay
            await self.backend.enqueue_delayed(self.name, job.to_dict(), job.delay_until)
        else:
            await self.backend.enqueue(self.name, job.to_dict())

        # Return the ID of the newly created job.
        return job.id

    async def add_bulk(self, jobs: list[dict[str, Any]]) -> list[str]:
        """
        Creates and enqueues multiple jobs onto this queue in a single batch operation.

        This method is more efficient than calling `add` for each job individually.
        Each dictionary in the `jobs` list must contain the necessary parameters
        to construct a `Job` instance, including at least "task_id".

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
        payloads: list[dict[str, Any]] = []
        # Iterate through job configurations, create Job objects, and prepare payloads.
        for cfg in jobs:
            job = Job(
                task_id=cfg.get("task_id"),
                args=cfg.get("args", []),
                kwargs=cfg.get("kwargs", {}),
                retries=0,
                max_retries=cfg.get("retries", 0),
                backoff=cfg.get("backoff"),
                ttl=cfg.get("ttl"),
                priority=cfg.get("priority", 5),
            )
            created_ids.append(job.id)
            payloads.append(job.to_dict())

        # Enqueue all job payloads in a single bulk operation via the backend.
        await self.backend.bulk_enqueue(self.name, payloads)

        # Return the list of IDs for the created jobs.
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

        Raises:
            ValueError: If neither `every` nor `cron` is provided.
        """
        # Validate that either 'every' or 'cron' is provided.
        if not every and not cron:
            raise ValueError("Either 'every' (seconds or string) or 'cron' (expression) must be " "provided.")

        # Create a dictionary representing the repeatable job entry.
        entry = {
            "task_id": task_id,
            "args": args or [],
            "kwargs": kwargs or {},
            "retries": retries,
            "ttl": ttl,
            "priority": priority,
        }
        # Add 'every' or 'cron' to the entry if provided.
        if every:
            entry["every"] = every
        if cron:
            entry["cron"] = cron

        # Append the repeatable job entry to the internal list.
        self._repeatables.append(entry)

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

    async def enqueue(self, payload: dict[str, Any]) -> None:
        """
        Enqueue a job for immediate processing.
        """
        await self.backend.enqueue(self.name, payload)

    async def enqueue_delayed(self, payload: dict[str, Any], run_at: float) -> None:
        """
        Schedule a job to run at a future UNIX timestamp.
        """
        await self.backend.enqueue_delayed(self.name, payload, run_at)

    async def delay(self, payload: dict[str, Any], run_at: float | None = None) -> None:
        """
        The same of enqueue with enqueue_delayed combined in one place.
        """
        if not run_at:
            await self.enqueue(payload)
        else:
            await self.enqueue_delayed(payload, run_at)

    async def get_due_delayed(self) -> list[dict[str, Any]]:
        """
        Pop & return any jobs whose run_at ≤ now.
        """
        return await self.backend.get_due_delayed(self.name)

    async def list_delayed(self) -> bool:
        return await self.backend.list_delayed(self.name)  # type: ignore

    async def remove_delayed(self, job_id: str) -> bool:
        return await self.backend.remove_delayed(self.name, job_id)  # type: ignore

    async def list_repeatables(self) -> list[RepeatableInfo]:
        return await self.backend.list_repeatables(self.name)

    async def pause_repeatable(self, job_def: dict[str, Any]) -> None:
        await self.backend.pause_repeatable(self.name, job_def)

    async def resume_repeatable(self, job_def: dict[str, Any]) -> float:
        return await self.backend.resume_repeatable(self.name, job_def)

    async def cancel_job(self, job_id: str) -> None:
        await self.backend.cancel_job(self.name, job_id)

    async def is_job_cancelled(self, job_id: str) -> bool:
        return await self.backend.is_job_cancelled(self.name, job_id)

    async def queue_stats(self) -> dict[str, int]:
        """
        Get counts of waiting, delayed, failed for this queue.
        """
        return await self.backend.queue_stats(self.name)

    async def list_jobs(self, state: str) -> list[dict[str, Any]]:
        return await self.backend.list_jobs(self.name, state)
