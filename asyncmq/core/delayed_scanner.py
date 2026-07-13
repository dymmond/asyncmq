import anyio

from asyncmq import monkay
from asyncmq.backends.base import BaseBackend
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
            jobs = await backend.promote_due_delayed(queue_name)
        except Exception:
            logger.error("Delayed job scanner failed for queue %r", queue_name, exc_info=True)
            await anyio.sleep(interval)
            continue

        for job_data in jobs:
            logger.info(f"[{job_data.get('id', '<unknown>')}] Moved delayed job to queue")

        await anyio.sleep(interval)
