import asyncio
import time

from asyncqueue.job import Job

_last_run: dict[str, float] = {}

async def repeatable_scheduler(backend, queue_name: str, jobs: list, interval: float = 1.0):
    while True:
        now = time.time()
        for job_def in jobs:
            task_id = job_def["task_id"]
            repeat_every = job_def["repeat_every"]
            key = f"{queue_name}:{task_id}"

            last_run = _last_run.get(key, 0)
            if now - last_run >= repeat_every:
                print(f"[scheduler] Enqueuing repeatable job {task_id}")
                job = Job(
                    task_id=task_id,
                    args=job_def.get("args", []),
                    kwargs=job_def.get("kwargs", {}),
                    max_retries=job_def.get("max_retries", 3),
                    ttl=job_def.get("ttl", None),
                    priority=job_def.get("priority", 5)
                )
                await backend.enqueue(queue_name, job.to_dict())
                _last_run[key] = now

        await asyncio.sleep(interval)
