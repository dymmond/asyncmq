import asyncio

from asyncqueue.job import Job


async def delayed_job_scanner(queue_name: str, backend, interval: float = 2.0):
    print(f"Delayed job scanner started for queue: {queue_name}")
    while True:
        jobs = await backend.get_due_delayed(queue_name)
        for job_data in jobs:
            job = Job.from_dict(job_data)
            await backend.remove_delayed(queue_name, job.id)
            await backend.enqueue(queue_name, job.to_dict())
            print(f"[{job.id}] Moved delayed job to queue")

        await asyncio.sleep(interval)
