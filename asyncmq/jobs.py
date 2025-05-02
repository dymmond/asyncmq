import time
import uuid
from typing import Any, Callable

from asyncmq.core.enums import State

# A tuple listing all possible states that a job can transition through
# during its lifecycle within the queue system.
JOB_STATES: tuple[str, ...] = (
    State.WAITING,
    State.ACTIVE,
    State.COMPLETED,
    State.FAILED,
    State.DELAYED,
    State.EXPIRED,
)


class Job:
    """
    Represents a single unit of work (a job) managed by the asyncmq queue system.

    A Job instance encapsulates all the necessary information to execute a task,
    track its state, manage retries, handle delays, and store results. It
    includes metadata such as the task ID, arguments, status, timestamps,
    and configuration for retry behavior and time-to-live.
    """

    def __init__(
        self,
        task_id: str,
        args: list[Any],
        kwargs: dict[str, Any],
        retries: int = 0,
        max_retries: int = 3,
        backoff: float | int | Callable | None = 1.5,
        ttl: int | None = None,
        job_id: str | None = None,
        created_at: float | None = None,
        priority: int = 5,
        repeat_every: float | int | None = None,
        depends_on: list[str] | None = None,
    ) -> None:
        """
        Initializes a new Job instance.

        Args:
            task_id: The unique identifier string for the task function that this
                     job is intended to execute. This ID is used to look up the
                     actual callable function in the task registry.
            args: A list of positional arguments to be passed to the task function
                  when the job is executed.
            kwargs: A dictionary of keyword arguments to be passed to the task
                    function.
            retries: The number of times this job has already been attempted
                     (usually 0 for a new job, incremented on failure). Defaults to 0.
            max_retries: The maximum number of times this job is allowed to be
                         retried before being marked as failed. Defaults to 3.
            backoff: Defines the strategy for calculating the delay before the
                     next retry attempt after a failure.
                     - If a number (int or float), the delay is calculated as
                       `backoff ** retries`.
                     - If a callable that accepts the retry count (`callable(retries)`),
                       the delay is the result of calling it with the current
                       retry count.
                     - If a callable that accepts no arguments (`callable()`),
                       the delay is the result of calling it.
                     - If None, there is no delay between retries (retries happen
                       immediately). Defaults to 1.5.
            ttl: The time-to-live (TTL) for the job in seconds, measured from
                 its creation time (`created_at`). If the current time exceeds
                 `created_at + ttl`, the job is considered expired. Defaults to None
                 (no TTL).
            job_id: An optional pre-assigned unique ID for this job. If None, a new
                    UUID is generated. Defaults to None.
            created_at: An optional timestamp (as a float, e.g., from time.time())
                        representing when the job was created. If None, the current
                        time is used. Defaults to None.
            priority: The priority level of the job. Lower numbers typically indicate
                      higher priority for processing. Defaults to 5.
            repeat_every: If set to a number (float or int), this job is a repeatable
                          job definition, and new job instances will be enqueued
                          periodically with this interval (in seconds) by the
                          repeatable scheduler. Defaults to None.
            depends_on: An optional list of job IDs (strings) that this job depends on.
                        This job will not be executed until all jobs listed in
                        `depends_on` have completed successfully. Defaults to None.
        """
        # Generate a unique ID if none is provided.
        self.id: str = job_id or str(uuid.uuid4())
        self.task_id: str = task_id
        self.args: list[Any] = args
        self.kwargs: dict[str, Any] = kwargs
        self.retries: int = retries
        self.max_retries: int = max_retries
        self.backoff: float | int | Callable | None = backoff
        self.ttl: int | None = ttl
        # Record creation time, defaulting to the current time if not provided.
        self.created_at: float = created_at or time.time()
        self.last_attempt: float | None = None
        self.status: str = State.WAITING
        self.result: Any = None
        self.delay_until: float | None = None
        self.priority: int = priority
        self.repeat_every: float | int | None = repeat_every
        # Ensure depends_on is always a list, defaulting to empty if None.
        self.depends_on: list[str] = depends_on or []

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Job":
        """
        Creates a Job instance from a dictionary representation.

        This static method is used to reconstruct a Job object from data
        typically loaded from a backend storage system. It maps keys from the
        dictionary back to Job attributes.

        Args:
            data: A dictionary containing the job's data and metadata,
                  expected to include keys like "id", "task", "args", "kwargs", etc.

        Returns:
            A new Job instance populated with the data from the dictionary.
        """
        # Create a Job instance, mapping dictionary keys to constructor arguments.
        job = Job(
            task_id=data["task"],  # Use "task" key for task_id.
            args=data.get("args", []),
            kwargs=data.get("kwargs", {}),
            retries=data.get("retries", 0),
            max_retries=data.get("max_retries", 3),
            backoff=data.get("backoff"),
            ttl=data.get("ttl"),
            job_id=data["id"],  # Use "id" key for job_id.
            created_at=data.get("created_at"),
            priority=data.get("priority", 5),
            repeat_every=data.get("repeat_every"),
            depends_on=data.get("depends_on", []),
        )
        # Set remaining attributes from the dictionary data.
        job.status = data.get("status", State.WAITING)
        job.result = data.get("result")
        job.delay_until = data.get("delay_until")
        job.last_attempt = data.get("last_attempt")
        # Return the fully populated Job instance.
        return job

    def is_expired(self) -> bool:
        """
        Checks if the job has expired based on its Time-to-Live (TTL).

        A job is considered expired if its `ttl` attribute is set (not None)
        and the current time is greater than the job's creation time plus its TTL.

        Returns:
            True if the job is expired, False otherwise.
        """
        # If TTL is not set, the job cannot expire based on TTL.
        if self.ttl is None:
            return False
        # Check if the current time is past the expiration time (created_at + ttl).
        return (time.time() - self.created_at) > self.ttl

    def next_retry_delay(self) -> float:
        """
        Computes the duration to wait before the next retry attempt for this job.

        The delay calculation follows the strategy defined by the `backoff`
        attribute:
        - If `backoff` is a number (int or float), the delay is `backoff ** current_retries`.
        - If `backoff` is a callable, it first attempts to call it with the
          current number of retries (`backoff(self.retries)`). If that fails
          (e.g., TypeError), it attempts to call it without arguments (`backoff()`).
        - If `backoff` is None or the callable could not be invoked successfully,
          the delay is 0.0 (immediate retry).

        Returns:
            The calculated delay in seconds as a float.
        """
        # If backoff is None, there is no delay.
        if self.backoff is None:
            return 0.0

        # If backoff is a number, calculate exponential backoff.
        if isinstance(self.backoff, (int, float)):
            try:
                # Calculate delay as backoff base raised to the power of retries.
                return float(self.backoff) ** self.retries
            except Exception:
                # Fallback to the base value if calculation fails (e.g., large retries).
                return float(self.backoff)

        # If backoff is a callable.
        if callable(self.backoff):
            try:
                # Try calling the callable with the retry count.
                return float(self.backoff(self.retries))
            except TypeError:
                # If calling with retry count fails, try calling without arguments.
                try:
                    return float(self.backoff())
                except Exception:
                    # If callable execution fails, return 0 delay.
                    return 0.0

        # If backoff is none of the expected types, return 0 delay.
        return 0.0  # type: ignore

    def to_dict(self) -> dict[str, Any]:
        """
        Serializes the Job instance into a dictionary representation.

        This method converts the Job object's attributes into a dictionary
        format suitable for storage in a backend or for transmission.

        Returns:
            A dictionary containing all relevant attributes of the Job instance.
        """
        # Return a dictionary containing key attributes of the Job instance.
        return {
            "id": self.id,
            "task": self.task_id,
            "args": self.args,
            "kwargs": self.kwargs,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "backoff": self.backoff,
            "ttl": self.ttl,
            "created_at": self.created_at,
            "last_attempt": self.last_attempt,
            "status": self.status,
            "result": self.result,
            "delay_until": self.delay_until,
            "priority": self.priority,
            "depends_on": self.depends_on,
            "repeat_every": self.repeat_every,
        }
