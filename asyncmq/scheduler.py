from typing import Any, Dict, List, Optional

import anyio

from asyncmq.job import Job


async def repeatable_scheduler(
    backend,
    queue_name: str,
    jobs: List[Dict[str, Any]],
    interval: Optional[float] = None,
) -> None:
    """
    Scheduler for repeatable jobs; enqueues jobs at their defined intervals.

    If `interval` is None, uses the minimum `repeat_every` among jobs.
    """
    last_run: Dict[str, float] = {}

    # Determine scan frequency
    if interval is None:
        positives = [
            jd.get("repeat_every", 0)
            for jd in jobs
            if jd.get("repeat_every", 0) > 0
        ]
        interval = min(positives) if positives else 1.0

    while True:
        now = anyio.current_time()  # monotonic clock
        for job_def in jobs:
            key = job_def["task_id"]
            repeat_every = job_def.get("repeat_every", 0)
            last = last_run.get(key, 0.0)
            if repeat_every and (now - last) >= repeat_every:
                job = Job(
                    task_id=job_def["task_id"],
                    args=job_def.get("args", []),
                    kwargs=job_def.get("kwargs", {}),
                    max_retries=job_def.get("max_retries", 3),
                    ttl=job_def.get("ttl"),
                    priority=job_def.get("priority", 5),
                    repeat_every=repeat_every,
                )
                await backend.enqueue(queue_name, job.to_dict())
                last_run[key] = now

        await anyio.sleep(interval)
