import time
from typing import Any, Dict, List, Optional

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import settings
from asyncmq.job import Job
from asyncmq.runner import run_worker


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
        backend: Optional[BaseBackend] = None,
        concurrency: int = 3,
        rate_limit: Optional[int] = None,
        rate_interval: float = 1.0,
    ) -> None:
        """
        Initializes a Queue instance.

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
        """
        self.name: str = name
        # Use the provided backend or instantiate RedisBackend if none is given.
        self.backend: BaseBackend = backend or settings.backend
        # A list to store definitions of repeatable jobs added to this queue.
        self._repeatables: List[Dict[str, Any]] = []
        self.concurrency: int = concurrency
        self.rate_limit: Optional[int] = rate_limit
        self.rate_interval: float = rate_interval

    async def add(
        self,
        task_id: str,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        retries: int = 0,
        ttl: Optional[int] = None,
        backoff: Optional[float] = None,
        priority: int = 5,
        delay: Optional[float] = None,
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
        # Create a new Job instance with the provided parameters.
        job: Job = Job(
            task_id=task_id,
            args=args or [],
            kwargs=kwargs or {},
            retries=0,  # Start with 0 retries for a new job.
            max_retries=retries,
            backoff=backoff,
            ttl=ttl,
            priority=priority,
        )
        # Check if a delay is specified for this job.
        if delay is not None:
            # If delayed, calculate the absolute time when the job should become available.
            job.delay_until = time.time() + delay
            # Enqueue the job with the calculated delay using the backend.
            await self.backend.enqueue_delayed(self.name, job.to_dict(), job.delay_until)
        else:
            # If no delay, enqueue the job for immediate processing.
            await self.backend.enqueue(self.name, job.to_dict())

        # Return the ID of the created job.
        return job.id

    async def add_bulk(self, jobs: List[Dict[str, Any]]) -> List[str]:
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
        created_ids: List[str] = []
        payloads: List[Dict[str, Any]] = []
        # Iterate through each job configuration dictionary in the input list.
        for cfg in jobs:
            # Create a Job instance from the configuration dictionary.
            job: Job = Job(
                task_id=cfg.get("task_id"), # task_id is expected to be present
                args=cfg.get("args", []),
                kwargs=cfg.get("kwargs", {}),
                retries=0, # New jobs start with 0 retries.
                max_retries=cfg.get("retries", 0), # Default max_retries to 0 if not specified.
                backoff=cfg.get("backoff"),
                ttl=cfg.get("ttl"),
                priority=cfg.get("priority", 5), # Default priority to 5 if not specified.
            )
            # Store the ID of the created job.
            created_ids.append(job.id)
            # Convert the Job instance to a dictionary payload for the backend.
            payloads.append(job.to_dict())

        # Use the backend to enqueue all job payloads in a single bulk operation.
        await self.backend.bulk_enqueue(self.name, payloads)

        # Return the list of IDs for the created jobs.
        return created_ids

    def add_repeatable(
        self,
        task_id: str,
        every: float,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        retries: int = 0,
        ttl: Optional[int] = None,
        priority: int = 5,
    ) -> None:
        """
        Registers a job definition to be scheduled and enqueued repeatedly
        by the worker's internal scheduler.

        This method does *not* immediately enqueue a job. Instead, it adds
        the job definition to an internal list (`_repeatables`). When the
        `run()` or `start()` method is called to start the worker, a separate
        scheduler task is launched which periodically checks these registered
        definitions and enqueues new jobs based on the `every` interval.

        Args:
            task_id: The unique identifier string for the task function to
                     execute for repeatable jobs.
            every: The time interval in seconds between each repeatable job
                   instance being enqueued.
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
        """
        # Append the repeatable job definition as a dictionary to the internal list.
        self._repeatables.append({
            "task_id": task_id,
            "args": args or [],
            "kwargs": kwargs or {},
            "repeat_every": every,  # Store the interval for repeating
            "max_retries": retries,
            "ttl": ttl,
            "priority": priority,
        })

    async def pause(self) -> None:
        """
        Signals the backend to pause job processing for this specific queue.

        Workers consuming from a paused queue should stop dequeueing new jobs
        until the queue is resumed. Jobs currently being processed might finish
        depending on the backend implementation and worker logic.
        """
        # Instruct the backend to pause the queue by name.
        await self.backend.pause_queue(self.name)

    async def resume(self) -> None:
        """
        Signals the backend to resume job processing for this specific queue.

        If the queue was previously paused, workers will begin dequeueing and
        processing jobs again after this method is called.
        """
        # Instruct the backend to resume the queue by name.
        await self.backend.resume_queue(self.name)

    async def clean(
        self,
        state: str,
        older_than: Optional[float] = None,
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
        # Instruct the backend to purge jobs in the specified state older than the given timestamp.
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
        # Call the run_worker function with the queue's configuration.
        # The worker will manage the job processing loops internally.
        await run_worker(
            self.name,
            self.backend,
            concurrency=self.concurrency,
            rate_limit=self.rate_limit,
            rate_interval=self.rate_interval,
            repeatables=self._repeatables, # Pass the list of repeatable job definitions
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
        # Use anyio.run to execute the asynchronous run() method.
        anyio.run(self.run)
