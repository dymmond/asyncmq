import asyncio
import time

from asyncmq.backends.redis import RedisBackend
from asyncmq.logging import logger
from asyncmq.queues import Queue
from asyncmq.tasks import task

backend = RedisBackend(redis_url_or_client="redis://localhost:6379/0")
queue = Queue(name="perf", backend=backend, concurrency=10)

@task(queue="perf")
async def noop():
    """A minimal task for pure overhead measurement."""
    return True

async def enqueue_jobs(n: int):
    for _ in range(n):
        await noop.enqueue(backend)

async def run_benchmark(n: int):
    start = time.time()
    await enqueue_jobs(n)
    end_enqueue = time.time()
    logger.info(f"Enqueued {n} jobs in {end_enqueue - start:.2f}s")

    # Process jobs
    await queue.run()
    end_process = time.time()
    logger.info(f"Processed {n} jobs in {end_process - end_enqueue:.2f}s")

if __name__ == "__main__":
    asyncio.run(run_benchmark(1000))
