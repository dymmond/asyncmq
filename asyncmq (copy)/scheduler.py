from typing import Any, Dict, List, Optional

import anyio

from asyncmq.job import Job


async def repeatable_scheduler(
    backend: Any,
    queue_name: str,
    jobs: List[Dict[str, Any]],
    interval: Optional[float] = None,
) -> None:
    """
    Schedules and enqueues jobs that are configured to repeat at defined intervals.

    This function runs an infinite loop that periodically checks a list of job
    definitions. For each job definition that has a `repeat_every` interval
    configured, it checks if the required time has passed since the last time
    a job for this definition was enqueued. If the interval has passed, a
    new job instance is created based on the definition and enqueued onto the
    specified queue using the provided backend.

    The frequency at which the scheduler checks for jobs to enqueue is determined
    by the `interval` parameter. If `interval` is not provided (None), the
    scheduler automatically determines the check frequency by finding the minimum
    positive `repeat_every` value among all the job definitions provided in the
    `jobs` list. This ensures that the scheduler wakes up frequently enough to
    enqueue even the most frequent repeatable jobs on time. If no jobs have
    a positive `repeat_every`, the default interval becomes 1.0 second.

    The scheduler uses `anyio.current_time()` for timekeeping, which is based
    on a monotonic clock, suitable for measuring time differences accurately
    even if the system clock is adjusted.

    Args:
        backend: An object providing the necessary interface for interacting
                 with the queue, specifically the `enqueue` method. Typed as `Any`.
        queue_name: The name of the queue where the repeatable jobs should be
                    enqueued.
        jobs: A list of dictionaries, where each dictionary represents a job
              definition that might be scheduled repeatedly. Each dictionary
              is expected to contain at least a "task_id" and optionally
              "repeat_every", "args", "kwargs", "max_retries", "ttl", and "priority".
        interval: The fixed interval (in seconds) at which the scheduler loop
                  should check for jobs to enqueue. If None, the interval is
                  calculated based on the minimum positive `repeat_every` of
                  the provided jobs. Defaults to None.
    """
    # Dictionary to store the last time each repeatable job definition was enqueued.
    # Key is the task ID (string), value is the timestamp (float) from anyio.current_time().
    last_run: Dict[str, float] = {}

    # Determine the frequency at which the scheduler loop should run.
    if interval is None:
        # Find all positive repeat_every values from the job definitions.
        positives = [
            jd.get("repeat_every", 0.0)  # Ensure float type for calculation
            for jd in jobs
            if jd.get("repeat_every", 0.0) > 0
        ]
        # If there are positive repeat intervals, use the minimum as the scheduler interval.
        # Otherwise, default to 1.0 second to prevent an infinite loop with zero sleep.
        calculated_interval: float = min(positives) if positives else 1.0
    else:
        # If an interval was provided, use it.
        calculated_interval = interval

    # The main scheduling loop runs indefinitely.
    while True:
        # Get the current time from a monotonic clock.
        now: float = anyio.current_time()  # monotonic clock
        # Iterate through each job definition provided.
        for job_def in jobs:
            # Get the unique identifier for the task.
            key: str = job_def["task_id"]
            # Get the repeat interval for this specific job definition, defaulting to 0.
            repeat_every: float = job_def.get("repeat_every", 0.0)
            # Get the time this job was last enqueued, defaulting to 0.0 if never run.
            last: float = last_run.get(key, 0.0)

            # Check if the job is configured to repeat and if the required time
            # has passed since the last time it was enqueued.
            if repeat_every > 0 and (now - last) >= repeat_every:
                # If it's time to repeat, create a new Job instance based on the definition.
                job = Job(
                    task_id=job_def["task_id"],
                    args=job_def.get("args", []),
                    kwargs=job_def.get("kwargs", {}),
                    max_retries=job_def.get("max_retries", 3), # Default retries if not specified in definition
                    ttl=job_def.get("ttl"),
                    priority=job_def.get("priority", 5), # Default priority if not specified
                    repeat_every=repeat_every,          # Include repeat_every in the new job instance
                )
                # Enqueue the newly created job instance onto the target queue using the backend.
                await backend.enqueue(queue_name, job.to_dict())
                # Update the last run time for this job definition to the current time.
                last_run[key] = now

        # Sleep for the calculated or specified interval before the next check cycle.
        await anyio.sleep(calculated_interval)
