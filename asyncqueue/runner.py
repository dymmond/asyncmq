import asyncio

from asyncqueue.delayed_scanner import delayed_job_scanner
from asyncqueue.rate_limiter import RateLimiter
from asyncqueue.scheduler import repeatable_scheduler
from asyncqueue.worker import process_job


async def run_worker(queue_name: str, backend, concurrency: int = 3, rate_limit: int = None, rate_interval: float = 1.0, repeatables: list = None):
    semaphore = asyncio.Semaphore(concurrency)
    rate_limiter = RateLimiter(rate_limit, rate_interval) if rate_limit else None

    tasks = [
        process_job(queue_name, backend, semaphore, rate_limiter=rate_limiter),
        delayed_job_scanner(queue_name, backend)
    ]

    if repeatables:
        tasks.append(repeatable_scheduler(backend, queue_name, repeatables))

    await asyncio.gather(*tasks)
if __name__ == "__main__":
    import asyncio

    from asyncqueue.backends.memory import InMemoryBackend

    backend = InMemoryBackend()
    asyncio.run(run_worker("emails", backend))
