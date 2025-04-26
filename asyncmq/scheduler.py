import asyncio
import time

from asyncmq.job import Job


async def repeatable_scheduler(backend, queue_name: str, jobs: list, interval: float = 1.0):
    """
    Scheduler for repeatable jobs; maintains a local last-run map per invocation.

    :param backend: Backend instance for enqueueing jobs
    :param queue_name: The queue to push repeatables onto
    :param jobs: List of job definitions with keys:
                 task_id, args, kwargs, repeat_every, (optional) max_retries, ttl, priority
    :param interval: How often to check for due repeatables
    """
    # Track last run time per repeatable key
    last_run: dict[str, float] = {}

    while True:
        now = time.time()
        for job_def in jobs:
            task_id = job_def["task_id"]
            repeat_every = job_def["repeat_every"]
            key = f"{queue_name}:{task_id}"

            prev = last_run.get(key, 0)
            # Schedule if never run before or past interval
            if now - prev >= repeat_every:
                # Enqueue repeatable job
                job = Job(
                    task_id=task_id,
                    args=job_def.get("args", []),
                    kwargs=job_def.get("kwargs", {}),
                    max_retries=job_def.get("max_retries", 3),
                    ttl=job_def.get("ttl", None),
                    priority=job_def.get("priority", 5),
                    repeat_every=repeat_every
                )
                await backend.enqueue(queue_name, job.to_dict())
                # Update last run timestamp
                last_run[key] = now

        await asyncio.sleep(interval)
