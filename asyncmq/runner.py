import asyncio
from typing import Any, List, Optional

from asyncmq.delayed_scanner import delayed_job_scanner
from asyncmq.rate_limiter import RateLimiter
from asyncmq.worker import process_job


async def run_worker(
    queue_name: str,
    backend: Any,  # The specific type of the backend object is not defined in the original code.
    concurrency: int = 3,
    rate_limit: Optional[int] = None,
    rate_interval: float = 1.0,
    repeatables: Optional[list] = None # The specific type of items in the list is not defined in the original code.
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
                 enqueuing, updating job states, etc.). Its specific type is
                 not explicitly defined in the original code.
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
    """
    # Create an asyncio Semaphore to limit the number of concurrent tasks.
    semaphore: asyncio.Semaphore = asyncio.Semaphore(concurrency)

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
        rate_limiter: Optional[Any] = _BlockAll() # Type hint for the special blocker instance.
    elif rate_limit is None:
        # If rate_limit is None, no rate limiting is applied.
        rate_limiter: Optional[Any] = None
    else:
        # If rate_limit is a positive integer, initialize the standard RateLimiter.
        rate_limiter = RateLimiter(rate_limit, rate_interval)

    # Build the list of core asynchronous tasks that constitute the worker.
    # 1. The main process_job task that pulls jobs from the queue and handles them.
    # 2. The delayed_job_scanner task that monitors and re-enqueues delayed jobs.
    tasks: List[Any] = [
        process_job(queue_name, backend, semaphore, rate_limiter=rate_limiter),
        delayed_job_scanner(queue_name, backend, interval=0.1)
    ]

    # If repeatable job definitions are provided, add the repeatable scheduler task.
    if repeatables:
        # Import the scheduler function here to avoid circular dependencies
        # if this module is imported elsewhere first.
        from asyncmq.scheduler import repeatable_scheduler
        # Add the repeatable scheduler task to the list of tasks to run.
        tasks.append(repeatable_scheduler(backend, queue_name, repeatables))

    # Use asyncio.gather to run all the created tasks concurrently.
    # This function will wait for all tasks to complete (which, for these
    # worker tasks, means running until cancelled or an error occurs).
    await asyncio.gather(*tasks)
