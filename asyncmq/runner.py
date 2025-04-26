import asyncio

from asyncmq.delayed_scanner import delayed_job_scanner
from asyncmq.rate_limiter import RateLimiter
from asyncmq.worker import process_job


async def run_worker(
    queue_name: str,
    backend,
    concurrency: int = 3,
    rate_limit: int | None = None,
    rate_interval: float = 1.0,
    repeatables: list = None
):
    """
    Launches a worker for processing jobs with optional rate limiting and repeatable tasks.

    :param queue_name: Name of the queue to consume from.
    :param backend: The backend instance implementing the queue interface.
    :param concurrency: Maximum number of concurrent job executions.
    :param rate_limit: Number of jobs allowed per rate_interval; if 0, blocks all; if None, disables rate limiting.
    :param rate_interval: Time window in seconds for rate limiting.
    :param repeatables: List of repeatable job definitions to schedule periodically.
    """
    # Semaphore to limit concurrent job executions
    semaphore = asyncio.Semaphore(concurrency)

    # Initialize rate limiter
    if rate_limit == 0:
        # Block all jobs when rate_limit is zero
        class _BlockAll:
            async def acquire(self):
                # never resolves, so jobs stay waiting
                await asyncio.Future()
        rate_limiter = _BlockAll()
    elif rate_limit is None:
        rate_limiter = None
    else:
        rate_limiter = RateLimiter(rate_limit, rate_interval)

    # Build the core tasks
    tasks = [
        process_job(queue_name, backend, semaphore, rate_limiter=rate_limiter),
        delayed_job_scanner(queue_name, backend, interval=0.1)
    ]

    # Optionally add the repeatable scheduler
    if repeatables:
        from asyncmq.scheduler import repeatable_scheduler
        tasks.append(repeatable_scheduler(backend, queue_name, repeatables))

    # Run until cancelled
    await asyncio.gather(*tasks)
