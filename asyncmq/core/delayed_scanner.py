import anyio

from asyncmq import monkay
from asyncmq.backends.base import BaseBackend
from asyncmq.jobs import Job
from asyncmq.logging import logger


async def delayed_job_scanner(
    queue_name: str,
    backend: BaseBackend | None = None,
    interval: float = 2.0,
) -> None:
    """
    Periodically scans the backend for delayed jobs that are due for processing
    and moves them to the active queue.

    This function runs continuously in an infinite loop, sleeping for a
    specified `interval` between scans. In each scan, it queries the `backend`
    for delayed jobs associated with `queue_name` that have passed their
    `delay_until` timestamp. For each due job found, it removes it from the
    backend's delayed storage and re-enqueues it into the main queue for
    processing by workers.

    Args:
        queue_name: The name of the queue whose delayed jobs should be scanned.
        backend: An object providing the necessary interface for interacting
                 with the queue storage, including methods like `get_due_delayed`,
                 `remove_delayed`, and `enqueue`. If `None`, the default backend
                 from `settings` is used.
        interval: The time in seconds to wait between consecutive scans for
                  due delayed jobs. Defaults to 2.0 seconds.
    """
    # Use the provided backend or fall back to the default configured backend.
    backend = backend or monkay.settings.backend
    logger.info(f"Delayed job scanner started for queue: {queue_name}")

    while True:
        try:
            jobs = await backend.pop_due_delayed(queue_name)
        except AttributeError:
            # Fallback for backends that don't support pop
            jobs = await backend.get_due_delayed(queue_name)
            for job_data in jobs:
                await backend.remove_delayed(queue_name, job_data["id"])

        for job_data in jobs:
            job = Job.from_dict(job_data)
            await backend.enqueue(queue_name, job.to_dict())
            logger.info(f"[{job.id}] Moved delayed job to queue")

        await anyio.sleep(interval)
