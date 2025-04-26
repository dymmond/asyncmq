import asyncio
import time


async def repeatable_scheduler(backend, queue_name: str, jobs: list = None, interval: float = 1.0):
    """
    Drives both in-memory and persistent repeatables.
    If `jobs` provided, registers them in Redis and seeds next_run.
    Then each interval, fetches due repeatables from Redis and enqueues.
    """
    # Seed repeatables into Redis store
    if jobs:
        now = time.time()
        for job_def in jobs:
            # schedule first run immediately if not already present
            await backend.add_repeatable(queue_name, job_def, now)

    while True:
        due = await backend.get_due_repeatables(queue_name)
        now = time.time()
        for job_def in due:
            # Enqueue the job
            print(f"[scheduler] Enqueuing repeatable job {job_def['task_id']}")
            from asyncmq.job import Job
            job = Job(
                task_id=job_def['task_id'],
                args=job_def.get('args', []),
                kwargs=job_def.get('kwargs', {}),
                max_retries=job_def.get('max_retries', 3),
                ttl=job_def.get('ttl', None),
                priority=job_def.get('priority', 5),
                repeat_every=job_def['repeat_every']
            )
            await backend.enqueue(queue_name, job.to_dict())
            # schedule next run
            next_run = now + job_def['repeat_every']
            await backend.add_repeatable(queue_name, job_def, next_run)
            # remove the old entry
            await backend.remove_repeatable(queue_name, job_def)

        await asyncio.sleep(interval)
