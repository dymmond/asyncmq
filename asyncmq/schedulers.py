import time
from typing import Any

import anyio

try:
    from croniter import croniter
except ImportError:
    raise ImportError("Please install croniter: pip install croniter") from None

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import settings
from asyncmq.jobs import Job


async def repeatable_scheduler(
    queue_name: str,
    jobs: list[dict[str, Any]],
    backend: BaseBackend | None = None,
    interval: float | None = None,
) -> None:
    """
    Schedules and enqueues jobs that are configured to repeat at defined intervals
    or according to cron expressions.

    Args:
        queue_name: The queue name.
        jobs: List of job definitions.
        backend: Backend to use.
        interval: How often the scheduler loop wakes up (seconds).
                  If None, auto-calculated based on job schedules.
    """
    backend = backend or settings.backend

    # Track last scheduled times for cron-based jobs
    cron_trackers: dict[str, croniter] = {}
    next_runs: dict[str, float] = {}

    # Precompute cron schedules
    for job in jobs:
        if "cron" in job:
            cron = job["cron"]
            itr = croniter(cron, time.time())
            cron_trackers[job["task_id"]] = itr
            next_runs[job["task_id"]] = itr.get_next()

    check_interval = interval or 30.0  # Wake up every 30s by default if not set

    while True:
        now = time.time()

        for job_def in jobs:
            task_id = job_def["task_id"]

            job_data = {
                "task_id": task_id,
                "args": job_def.get("args", []),
                "kwargs": job_def.get("kwargs", {}),
                "retries": job_def.get("retries", 0),
                "max_retries": job_def.get("retries", 0),
                "ttl": job_def.get("ttl"),
                "priority": job_def.get("priority", 5),
            }

            if "cron" in job_def:
                # Cron-based scheduling
                next_run = next_runs.get(task_id, 0)
                if now >= next_run:
                    job = Job(**job_data)

                    await backend.enqueue(queue_name, job.to_dict())
                    # Update next run
                    itr = cron_trackers[task_id]
                    next_runs[task_id] = itr.get_next()

            elif "every" in job_def:
                # Fixed-interval scheduling
                if "_last_run" not in job_def:
                    job_def["_last_run"] = now

                last_run = job_def["_last_run"]
                every = job_def["every"]
                if now - last_run >= every:
                    job = Job(**job_data)

                    await backend.enqueue(queue_name, job.to_dict())
                    job_def["_last_run"] = now

        await anyio.sleep(check_interval)
