import asyncio
import traceback

from asyncmq.event import event_emitter
from asyncmq.job import Job
from asyncmq.task import TASK_REGISTRY


async def process_job(queue_name: str, backend, semaphore: asyncio.Semaphore, rate_limiter=None):
    while True:
        async with semaphore:
            raw_job = await backend.dequeue(queue_name)
            if not raw_job:
                await asyncio.sleep(0.1)
                continue

            asyncio.create_task(handle_job(queue_name, backend, raw_job, rate_limiter=rate_limiter))


async def handle_job(queue_name: str, backend, raw_job: dict, rate_limiter=None):
    job = Job.from_dict(raw_job)

    if job.is_expired():
        print(f"[{job.id}] Job expired. Moving to DLQ.")
        job.status = "expired"
        await event_emitter.emit("job:expired", job.to_dict())
        await backend.move_to_dlq(queue_name, job.to_dict())
        return

    # Check dependencies
    if job.depends_on:
        all_done = True
        for dep_id in job.depends_on:
            state = await backend.get_job_state(queue_name, dep_id)
            if state != "completed":
                all_done = False
                break

        if not all_done:
            print(f"[{job.id}] Dependencies not met. Requeuing...")
            await asyncio.sleep(1)
            await backend.enqueue(queue_name, job.to_dict())
            await event_emitter.emit("job:requeued", job.to_dict())
            return

    try:
        if rate_limiter:
            await rate_limiter.acquire()

        job.status = "active"
        await backend.update_job_state(queue_name, job.id, "active")
        await event_emitter.emit("job:started", job.to_dict())

        task_func = TASK_REGISTRY[job.task_id]["func"]
        result = await task_func(*job.args, **job.kwargs)

        job.status = "completed"
        job.result = result
        await backend.ack(queue_name, job.id)
        await backend.save_job_result(queue_name, job.id, result)
        await backend.update_job_state(queue_name, job.id, "completed")
        await event_emitter.emit("job:completed", job.to_dict())

    except Exception:
        traceback.print_exc()
        job.retries += 1
        job.last_attempt = asyncio.get_event_loop().time()

        if job.retries > job.max_retries:
            job.status = "failed"
            await backend.move_to_dlq(queue_name, job.to_dict())
            await event_emitter.emit("job:failed", job.to_dict())
        else:
            delay = job.next_retry_delay()
            print(f"[{job.id}] Retry in {delay:.2f}s (retry {job.retries})")
            await asyncio.sleep(delay)
            job.status = "waiting"
            await backend.enqueue(queue_name, job.to_dict())
            await backend.update_job_state(queue_name, job.id, "waiting")
            await event_emitter.emit("job:requeued", job.to_dict())
