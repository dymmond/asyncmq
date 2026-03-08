import time
from typing import Any, cast

import anyio
from croniter import croniter

import asyncmq
from asyncmq.backends.base import BaseBackend, RepeatableInfo
from asyncmq.core.repeatables import build_repeatable_job, normalize_repeatable_job_def


async def repeatable_scheduler(
    queue_name: str,
    jobs: list[dict[str, Any]] | None,
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
    # Use the provided backend or fall back to the one from settings.
    #
    # The scheduler must not mutate unrelated worker settings. In particular,
    # sandbox execution is a worker concern and needs to remain whatever the
    # caller configured so job execution tests and production workers observe
    # the same behavior.
    backend = backend or asyncmq.monkay.settings.backend
    local_jobs = [normalize_repeatable_job_def(job) for job in (jobs or [])]

    # Dictionaries to keep track of cron iterators and their next run times
    cron_trackers: dict[str, croniter] = {}
    next_runs: dict[str, float] = {}

    # Initialize cron iterators and determine the initial next run time for each
    for job in local_jobs:
        if "cron" in job:
            cron = job["cron"]
            # Create a croniter instance starting from the current time
            itr = croniter(cron, time.time())
            cron_trackers[_local_repeatable_key(job)] = itr
            # Get the timestamp of the very next scheduled run
            next_runs[_local_repeatable_key(job)] = itr.get_next(float)  # Explicitly ask for float

    # Determine the interval for how often the scheduler loop checks
    # Use the specified interval or a default of 30 seconds
    check_interval = interval or 30.0

    # Main scheduler loop
    while True:
        # Get the current time once per loop iteration for consistency
        now = time.time()

        # Iterate through each job definition to check if it's time to schedule
        for job_def in local_jobs:
            repeatable_key = _local_repeatable_key(job_def)

            # Handle cron-based scheduling
            if "cron" in job_def:
                # Get the cron iterator and the previously calculated next run time
                itr = cron_trackers[repeatable_key]
                next_run = next_runs[repeatable_key]
                # Check if the current time is on or after the next scheduled time
                if now >= next_run:
                    await _enqueue_repeatable_job(queue_name, job_def, backend)
                    # Calculate the next scheduled run time for this cron job
                    next_runs[repeatable_key] = itr.get_next(float)  # Explicitly ask for float

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
                    await _enqueue_repeatable_job(queue_name, job_def, backend)
                    # Update the last run time to the current time
                    job_def["_last_run"] = now

        backend_next_run = await _process_backend_repeatables(queue_name, backend)

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
        if backend_next_run is not None:
            time_to_sleep = max(0.1, min(time_to_sleep, backend_next_run - now))

        # Asynchronously sleep before the next check
        await anyio.sleep(time_to_sleep)


async def _enqueue_repeatable_job(queue_name: str, job_def: dict[str, Any], backend: BaseBackend) -> None:
    """
    Enqueue one concrete job occurrence generated from a repeatable definition.

    Args:
        queue_name: The queue that should receive the generated job.
        job_def: The repeatable definition that produced this occurrence.
        backend: The backend used to enqueue the generated job.
    """
    job = build_repeatable_job(job_def)
    await backend.enqueue(queue_name, job.to_dict())


def _local_repeatable_key(job_def: dict[str, Any]) -> str:
    """
    Return a stable in-process key for tracking local repeatable schedules.

    Args:
        job_def: The local repeatable definition.

    Returns:
        A key used to track cron iterators and next-run timestamps.
    """
    return f"{job_def.get('task_id')}:{job_def.get('cron') or job_def.get('every')}"


async def _process_backend_repeatables(queue_name: str, backend: BaseBackend) -> float | None:
    """
    Trigger and advance backend-managed repeatables that are due.

    Args:
        queue_name: The queue whose backend repeatables should be processed.
        backend: The backend holding durable repeatable definitions.

    Returns:
        The earliest next-run timestamp still pending in the backend, if any.
    """
    get_due_repeatables = getattr(backend, "get_due_repeatables", None)
    advance_repeatable = getattr(backend, "advance_repeatable", None)
    list_repeatables = getattr(backend, "list_repeatables", None)

    if callable(get_due_repeatables) and callable(advance_repeatable):
        due_repeatables = await cast(Any, get_due_repeatables)(queue_name)
        for record in due_repeatables:
            repeatable = cast(RepeatableInfo, record)
            await _enqueue_repeatable_job(queue_name, repeatable.job_def, backend)
            await cast(Any, advance_repeatable)(queue_name, repeatable.job_def)

    if not callable(list_repeatables):
        return None

    repeatables = await cast(Any, list_repeatables)(queue_name)
    next_runs = [record.next_run for record in repeatables if not record.paused]
    if not next_runs:
        return None
    return min(next_runs)


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
