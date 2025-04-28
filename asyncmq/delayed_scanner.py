from typing import Any, Dict, List

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import settings
from asyncmq.job import Job


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
                 `remove_delayed`, and `enqueue`. Typed as `Any`.
        interval: The time in seconds to wait between consecutive scans for
                  due delayed jobs. Defaults to 2.0 seconds.
    """
    backend = backend or settings.backend
    # Print a message indicating the scanner has started (as in original code).
    print(f"Delayed job scanner started for queue: {queue_name}")

    # The scanner runs in an infinite loop.
    while True:
        # Retrieve delayed jobs that are due from the backend.
        jobs: List[Dict[str, Any]] = await backend.get_due_delayed(queue_name)

        # Iterate through each delayed job that is now due.
        for job_data in jobs:
            # Convert the raw job data dictionary into a Job instance.
            job: Job = Job.from_dict(job_data)
            # Remove the job from the backend's delayed storage.
            await backend.remove_delayed(queue_name, job.id)
            # Enqueue the job into the main processing queue.
            await backend.enqueue(queue_name, job.to_dict())
            # Print a message indicating the job was moved (as in original code).
            print(f"[{job.id}] Moved delayed job to queue")

        # Pause execution for the specified interval before the next scan.
        # anyio.sleep provides a portable sleep across different async backends (asyncio, Trio, etc.).
        await anyio.sleep(interval)
