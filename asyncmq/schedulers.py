import time
from typing import Any

import anyio
from croniter import croniter

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import monkay
from asyncmq.jobs import Job


async def repeatable_scheduler(
    queue_name: str,
    jobs: list[dict[str, Any]],
    backend: BaseBackend | None = None,
    interval: float | None = None,
) -> None:
    """
    Continuously schedules and enqueues jobs based on their repetition configurations.

    This asynchronous function runs indefinitely, waking up periodically to check
    a list of defined jobs. It supports two types of repetition:
    1.  `cron`: Jobs scheduled using a cron expression (e.g., "* * * * *").
    2.  `every`: Jobs scheduled at fixed intervals (e.g., every 60 seconds).

    When a job's scheduled time arrives, a corresponding Job object is created
    and enqueued onto the specified queue using the provided backend.

    The scheduler loop's wake-up interval can be configured. If not explicitly
    set, it defaults to 30 seconds, but may also be influenced by the next
    scheduled run time of the cron jobs to ensure accuracy.

    Args:
        queue_name: The name of the message queue where scheduled jobs will be enqueued.
        jobs: A list of dictionaries, where each dictionary defines a job,
              including 'task_id', optional 'args', 'kwargs', and either
              'cron' (str) or 'every' (float) keys for scheduling.
              Additional keys like 'retries', 'max_retries', 'ttl', and
              'priority' are also supported and passed to the Job object.
        backend: The backend instance (inheriting from BaseBackend) to use
                 for enqueuing jobs. If None, the backend configured in
                 asyncmq.conf.settings is used.
        interval: The minimum time in seconds the scheduler loop should sleep
                  between checks. If None, a default is used, and the actual
                  sleep time might be shorter to meet the next cron schedule.
                  This argument is advisory and aims to prevent excessive CPU usage.
    """
    # Use the provided backend or fall back to the one from settings
    backend = backend or monkay.settings.backend
    monkay.settings.sandbox_enabled = False

    # Dictionaries to keep track of cron iterators and their next run times
    cron_trackers: dict[str, croniter] = {}
    next_runs: dict[str, float] = {}

    # Initialize cron iterators and determine the initial next run time for each
    for job in jobs:
        if "cron" in job:
            cron = job["cron"]
            # Create a croniter instance starting from the current time
            itr = croniter(cron, time.time())
            cron_trackers[job["task_id"]] = itr
            # Get the timestamp of the very next scheduled run
            next_runs[job["task_id"]] = itr.get_next(float)  # Explicitly ask for float

    # Determine the interval for how often the scheduler loop checks
    # Use the specified interval or a default of 30 seconds
    check_interval = interval or 30.0

    # Main scheduler loop
    while True:
        # Get the current time once per loop iteration for consistency
        now = time.time()

        # Iterate through each job definition to check if it's time to schedule
        for job_def in jobs:
            task_id = job_def["task_id"]

            # Prepare the base data for the Job object
            job_data = {
                "task_id": task_id,
                "args": job_def.get("args", []),
                "kwargs": job_def.get("kwargs", {}),
                "retries": job_def.get("retries", 0),
                "max_retries": job_def.get("max_retries", 3),
                "ttl": job_def.get("ttl"),
                "priority": job_def.get("priority", 5),
            }

            # Handle cron-based scheduling
            if "cron" in job_def:
                # Get the cron iterator and the previously calculated next run time
                itr = cron_trackers[task_id]
                next_run = next_runs[task_id]
                # Check if the current time is on or after the next scheduled time
                if now >= next_run:
                    # Create a Job object with the defined data
                    job = Job(**job_data)  # type: ignore
                    # Enqueue the job using the backend
                    await backend.enqueue(queue_name, job.to_dict())
                    # Calculate the next scheduled run time for this cron job
                    next_runs[task_id] = itr.get_next(float)  # Explicitly ask for float

            # Handle fixed-interval scheduling
            elif "every" in job_def:
                # Initialize the last run time if this is the first check
                if "_last_run" not in job_def:
                    job_def["_last_run"] = now

                # Get the last run time and the required interval
                last_run = job_def["_last_run"]
                every = job_def["every"]
                # Check if the required interval has passed since the last run
                if now - last_run >= every:
                    # Create a Job object with the defined data
                    job = Job(**job_data)  # type: ignore

                    # Enqueue the job using the backend
                    await backend.enqueue(queue_name, job.to_dict())
                    # Update the last run time to the current time
                    job_def["_last_run"] = now

        # Calculate the time until the next event (either check_interval or next cron run)
        # This helps ensure the scheduler doesn't miss nearby cron schedules
        time_to_sleep = check_interval
        if next_runs:
            # Find the earliest next scheduled time among all cron jobs
            earliest_next_run = min(next_runs.values())
            # Calculate the time difference until the earliest next run
            time_until_next_event = earliest_next_run - now
            # Sleep for the minimum of the check_interval and the time until the next event
            # Ensure sleep time is not negative if time has passed
            time_to_sleep = max(0.1, min(check_interval, time_until_next_event))  # Sleep at least 0.1s

        # Asynchronously sleep before the next check
        await anyio.sleep(time_to_sleep)


def compute_next_run(job_def: dict[str, Any]) -> Any:
    """
    Given a repeatable job definition dict with either:
      - job_def["cron"]: a cron string
      - job_def["every"]: an interval in seconds
    returns the next UNIX timestamp (float).
    """
    now = time.time()
    if "cron" in job_def:
        # Use croniter to compute the next run after now
        itr = croniter(job_def["cron"], now)
        return itr.get_next(float)
    elif "every" in job_def:
        # Simply schedule 'every' seconds from now
        return now + float(job_def["every"])
    else:
        raise ValueError("Cannot compute next run: job_def lacks 'cron' or 'every'")
