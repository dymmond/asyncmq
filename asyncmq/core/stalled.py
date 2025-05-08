import time
from typing import TYPE_CHECKING, Any

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import monkay

if TYPE_CHECKING:
    from asyncmq.backends.base import BaseBackend


async def record_heartbeat(queue_name: str, job_id: str, backend: BaseBackend | None = None) -> None:
    """
    Record the timestamp of the last heartbeat for a running job.
    """
    backend = backend or monkay.settings.backend
    timestamp = time.time()
    await backend.save_heartbeat(queue_name, job_id, timestamp)


async def get_stalled_jobs(threshold: float, backend: BaseBackend | None = None) -> list[dict[str, Any]]:
    """
    Retrieve all jobs whose last heartbeat is older than now - threshold.
    Returns a list of dicts with keys 'queue' and 'job_data'.
    """
    backend = backend or monkay.settings.backend
    cutoff = time.time() - threshold
    return await backend.fetch_stalled_jobs(cutoff)


async def stalled_recovery_scheduler(
    backend: BaseBackend | None = None, check_interval: float | None = None, threshold: float | None = None
) -> None:
    """
    Periodically checks for stalled jobs and re-enqueues them.
    """
    backend = backend or monkay.settings.backend
    check_interval = check_interval or monkay.settings.stalled_check_interval
    threshold = threshold or monkay.settings.stalled_threshold

    while True:
        cutoff = time.time() - threshold
        # Fetch jobs whose heartbeat is older than cutoff
        stalled: list[dict[str, Any]] = await backend.fetch_stalled_jobs(cutoff)
        for entry in stalled:
            queue_name = entry["queue_name"]
            job_data = entry["job_data"]
            # Re-enqueue the stalled job
            await backend.reenqueue_stalled(queue_name, job_data)
            # Emit a stalled event, including queue_name in payload
            event_data = {"queue_name": queue_name, **job_data}
            await backend.emit_event("job:stalled", event_data)

        await anyio.sleep(check_interval)
