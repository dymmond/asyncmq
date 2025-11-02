import asyncio
import time
import uuid
from typing import Any

import anyio
from anyio import CapacityLimiter

from asyncmq import monkay
from asyncmq.backends.base import BaseBackend
from asyncmq.core.delayed_scanner import delayed_job_scanner
from asyncmq.core.lifecycle import run_hooks, run_hooks_safely
from asyncmq.rate_limiter import RateLimiter
from asyncmq.workers import Worker, handle_job, process_job


async def worker_loop(queue_name: str, backend: BaseBackend | None = None) -> None:
    """
    Continuously dequeues and processes jobs from a specified queue.

    This function represents a single worker instance that enters an infinite loop
    to fetch and handle jobs. It attempts to dequeue a job from `queue_name` using
    the provided `backend` (or the default backend if none is specified).
    If a job is found, it's passed to `handle_job` for processing. A small
    asynchronous sleep is included in each iteration to prevent busy-waiting
    and allow for other tasks to run.

    !!! Note
        This function is still in analysis and it might go away in the future.

    Args:
        queue_name: The name of the queue from which to dequeue jobs.
        backend: An optional backend instance to use for job operations.
                 If `None`, `monkay.settings.backend` will be used.

    Returns:
        None: This function runs indefinitely.
    """
    backend = backend or monkay.settings.backend

    while True:
        job = await backend.dequeue(queue_name)
        if job:
            await handle_job(queue_name, job, backend)
        await asyncio.sleep(0.1)  # Small sleep to avoid busy waiting


async def start_worker(queue_name: str, concurrency: int = 1) -> None:
    """
    Starts multiple worker instances for a specified message queue.

    This function initializes a `Worker` object for the given `queue_name`,
    sets its concurrency level, and then starts it. The heartbeat interval
    for the worker is configured using `monkay.settings.heartbeat_ttl`.

    Args:
        queue_name: The name of the message queue the workers will process.
        concurrency: The number of concurrent worker processes to run for this queue.
                     Defaults to 1.

    Returns:
        None
    """
    worker = Worker(queue_name, heartbeat_interval=monkay.settings.heartbeat_ttl)
    worker.concurrency = concurrency
    await worker.run()


async def run_worker(
    queue_name: str,
    backend: BaseBackend | None = None,
    concurrency: int = 3,
    rate_limit: int | None = None,
    rate_interval: float = 1.0,
    repeatables: list[Any] | None = None,
    scan_interval: float | None = None,
) -> None:
    """
    Launches and manages a worker process responsible for consuming and
    processing jobs from a specified queue.

    This function sets up the core components of the worker, including
    concurrency control via a semaphore, optional rate limiting, and
    integrates a scanner for delayed jobs. It can also optionally include
    a scheduler for repeatable tasks based on the provided definitions.
    The worker runs indefinitely, processing jobs until explicitly cancelled
    (e.g., by cancelling the task running this function).

    Args:
        queue_name: The name of the queue from which the worker will pull and
                    process jobs.
        backend: An object that provides the interface for interacting with the
                 underlying queue storage mechanism (e.g., methods for dequeueing,
                 enqueuing, updating job states, etc.). If `None`, the default
                 backend from `settings` is used.
        concurrency: The maximum number of jobs that can be processed
                     simultaneously by this worker. This is controlled by an
                     asyncio Semaphore. Defaults to 3.
        rate_limit: Configures rate limiting for job processing.
                    - If None (default), rate limiting is disabled.
                    - If an integer > 0, jobs are processed at a maximum rate
                      of `rate_limit` jobs per `rate_interval`.
                    - If 0, job processing is effectively blocked (a special
                      rate limiter that never acquires is used).
        rate_interval: The time window in seconds over which the `rate_limit`
                       applies. Defaults to 1.0 second.
        repeatables: An optional list of job definitions (dictionaries) that
                     should be scheduled periodically. If provided, a separate
                     scheduler task is started to enqueue these jobs based on
                     their configured `repeat_every` interval. The specific
                     structure of the dictionaries is expected by the
                     `repeatable_scheduler`. Defaults to None.
        scan_interval: How often to poll for delayed and repeatable jobs.
                       If None, uses monkay.settings.scan_interval.
    """
    # Use the provided backend or fall back to the default configured backend.
    backend = backend or monkay.settings.backend
    scan_interval = scan_interval or monkay.settings.scan_interval

    # Use anyio's CapacityLimiter to coordinate with process_job
    limiter: CapacityLimiter = CapacityLimiter(concurrency)

    worker_id = str(uuid.uuid4())
    timestamp = time.time()

    # Run lifecycle startup hooks for this worker
    await run_hooks(
        monkay.settings.worker_on_startup,
        backend=backend,
        worker_id=worker_id,
        queue=queue_name,
    )
    await backend.register_worker(worker_id, queue_name, concurrency, timestamp)

    try:
        # Initialize the rate limiter based on the configuration.
        if rate_limit == 0:
            # If rate_limit is 0, use a special internal class that never acquires,
            # effectively blocking all job processing attempts.
            class _BlockAll:
                """
                A dummy rate limiter implementation used to block all calls to
                acquire, effectively pausing job processing.
                """

                async def acquire(self) -> None:
                    """
                    Asynchronously attempts to acquire a permit. This implementation
                    creates a Future that is never resolved, causing any caller to
                    await indefinitely.
                    """
                    # Create a future that will never complete, blocking the caller.
                    await asyncio.Future()

            # Assign the special blocker instance.
            rate_limiter: Any | None = _BlockAll()
        elif rate_limit is None:
            # If rate_limit is None, no rate limiting is applied.
            rate_limiter: Any | None = None  # type: ignore
        else:
            # If rate_limit is a positive integer, initialize the standard RateLimiter.
            rate_limiter = RateLimiter(rate_limit, rate_interval)

        # Build the list of core asynchronous tasks that constitute the worker.
        # 1. The main process_job task that pulls jobs from the queue and handles them.
        # 2. The delayed_job_scanner task that monitors and re-enqueues delayed jobs.
        tasks: list[Any] = [
            process_job(queue_name, limiter, backend=backend, rate_limiter=rate_limiter),
            delayed_job_scanner(queue_name, backend, interval=scan_interval),
        ]

        # If repeatable job definitions are provided, add the repeatable scheduler task.
        if repeatables:
            # Import the scheduler function here to avoid circular dependencies
            # if this module is imported elsewhere first.
            from asyncmq.schedulers import repeatable_scheduler

            # Add the repeatable scheduler task to the list of tasks to run.
            tasks.append(repeatable_scheduler(queue_name, repeatables, backend=backend, interval=scan_interval))

        # Use asyncio.gather to run all the created tasks concurrently.
        # This function will wait for all tasks to complete (which, for these
        # worker tasks, means running until cancelled or an error occurs).
        await asyncio.gather(*tasks)
    except (anyio.get_cancelled_exc_class(), Exception):
        await backend.deregister_worker(worker_id)
        raise
    finally:
        # Always attempt to deregister and then run shutdown hooks
        try:
            await backend.deregister_worker(worker_id)
        finally:
            await run_hooks_safely(
                monkay.settings.worker_on_shutdown,
                backend=backend,
                worker_id=worker_id,
                queue=queue_name,
            )
