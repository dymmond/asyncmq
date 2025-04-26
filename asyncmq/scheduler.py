import asyncio
import time

from asyncmq.job import Job

async def repeatable_scheduler(
    backend,
    queue_name: str,
    jobs: list,
    interval: float | None = None,
):
    """
    Scheduler for repeatable jobs; maintains a local last-run map.

    :param backend: Backend instance for enqueueing jobs
    :param queue_name: The queue to push repeatables onto
    :param jobs: List of dicts with keys:
                 task_id, args, kwargs, repeat_every, max_retries, ttl, priority
    :param interval: How often to scan. If None, uses min(repeat_every).
    """
    last_run: dict = {}

    # Determine scan interval
    if interval is None:
        interval = min(job_def["repeat_every"] for job_def in jobs)

    while True:
        now = time.time()
        for job_def in jobs:
            key = job_def["task_id"]
            repeat_every = job_def["repeat_every"]
            last = last_run.get(key, 0.0)

            if now - last >= repeat_every:
                job = Job(
                    task_id=job_def["task_id"],
                    args=job_def.get("args", []),
                    kwargs=job_def.get("kwargs", {}),
                    max_retries=job_def.get("max_retries", 3),
                    ttl=job_def.get("ttl", None),
                    priority=job_def.get("priority", 5),
                    repeat_every=repeat_every
                )
                await backend.enqueue(queue_name, job.to_dict())
                last_run[key] = now

        await asyncio.sleep(interval)
