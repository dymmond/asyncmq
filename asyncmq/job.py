import time
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

from asyncmq.enums import State

# Define a type hint for the JOB_STATES tuple
JOB_STATES: Tuple[str, ...] = (State.WAITING, State.ACTIVE, State.COMPLETED, State.FAILED, State.DELAYED, State.EXPIRED)
"""
A tuple listing all possible states that a job can transition through
during its lifecycle within the queue system.
"""


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
        args: List[Any],
        kwargs: Dict[str, Any],
        retries: int = 0,
        max_retries: int = 3,
        backoff: Union[float, Any, None] = 1.5, # Original hint includes Any, suggesting flexibility.
        ttl: Optional[int] = None,
        job_id: Optional[str] = None,
        created_at: Optional[float] = None,
        priority: int = 5,
        repeat_every: Optional[Union[float, int]] = None,
        depends_on: Optional[List[str]] = None, # Corrected type hint to List[str] based on usage
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
                     - If None, there is no delay between retries (retries happen immediately).
                     Defaults to 1.5. Note: Original type hint includes `Any` which
                     suggests other callable types might be supported.
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
        # Assign or generate a unique ID for the job.
        self.id: str = job_id or str(uuid.uuid4())
        self.task_id: str = task_id
        # Store the arguments and keyword arguments for task execution.
        self.args: List[Any] = args
        self.kwargs: Dict[str, Any] = kwargs
        # Store retry information.
        self.retries: int = retries
        self.max_retries: int = max_retries
        self.backoff: Union[float, Any, None] = backoff
        # Store Time-to-Live.
        self.ttl: Optional[int] = ttl
        # Record creation timestamp.
        self.created_at: float = created_at or time.time()
        # Timestamp of the last failed attempt (None initially).
        self.last_attempt: Optional[float] = None
        # Current status of the job.
        self.status: str = State.WAITING
        # Result of a completed job (None initially or on failure).
        self.result: Any = None
        # Absolute time until which the job should be delayed (None unless delayed).
        self.delay_until: Optional[float] = None
        # Job priority.
        self.priority: int = priority
        # Repeat interval for repeatable jobs.
        self.repeat_every: Optional[Union[float, int]] = repeat_every
        # List of job IDs this job depends on.
        self.depends_on: List[str] = depends_on or []

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Job":
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
        # Create a Job instance using values from the dictionary.
        # Use .get() with defaults for optional keys or keys that might not
        # be present in older data formats.
        job: Job = Job(
            task_id=data["task"], # "task" key is expected
            args=data.get("args", []), # Default to empty list if missing
            kwargs=data.get("kwargs", {}), # Default to empty dict if missing
            retries=data.get("retries", 0), # Default to 0 if missing
            max_retries=data.get("max_retries", 3), # Default to 3 if missing
            backoff=data.get("backoff"),
            ttl=data.get("ttl"),
            job_id=data["id"], # "id" key is expected
            created_at=data.get("created_at"),
            priority=data.get("priority", 5), # Default to 5 if missing
            repeat_every=data.get("repeat_every"),
            depends_on=data.get("depends_on", []), # Default to empty list if missing
        )
        # Assign status, result, delay_until, and last_attempt separately
        # as they represent the job's dynamic state.
        job.status = data.get("status", State.WAITING)
        job.result = data.get("result")
        job.delay_until = data.get("delay_until")
        job.last_attempt = data.get("last_attempt")
        # Return the reconstructed Job instance.
        return job

    def is_expired(self) -> bool:
        """
        Checks if the job has expired based on its Time-to-Live (TTL).

        A job is considered expired if its `ttl` attribute is set (not None)
        and the current time is greater than the job's creation time plus its TTL.

        Returns:
            True if the job is expired, False otherwise.
        """
        # If TTL is not set, the job cannot expire.
        if self.ttl is None:
            return False
        # Calculate if the current time is past the expiration time (created_at + ttl).
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
        # If no backoff is configured, retry immediately.
        if self.backoff is None:
            return 0.0

        # Handle numeric backoff (int or float).
        if isinstance(self.backoff, (int, float)):
            try:
                # Calculate delay using backoff raised to the power of the retry count.
                return float(self.backoff) ** self.retries
            except Exception:
                # Fallback to the backoff value itself if the power calculation fails.
                return float(self.backoff)

        # Handle callable backoff.
        if callable(self.backoff):
            try:
                # Attempt to call the backoff function with the retry count.
                return float(self.backoff(self.retries))
            except TypeError:
                try:
                    # If calling with retry count fails, attempt to call without arguments.
                    return float(self.backoff())
                except Exception:
                    # If the callable cannot be invoked, fall back to no delay.
                    return 0.0

        # If backoff is not a number or callable, fall back to no delay.
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the Job instance into a dictionary representation.

        This method converts the Job object's attributes into a dictionary
        format suitable for storage in a backend or for transmission.

        Returns:
            A dictionary containing all relevant attributes of the Job instance.
        """
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
