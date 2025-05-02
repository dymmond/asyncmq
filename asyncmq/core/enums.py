from enum import Enum


class StrEnum(str, Enum):
    """
    A base class for creating enumerations whose members are also subclasses of `str`.

    This allows enum members to be used directly as strings while also providing
    the benefits of an Enum (e.g., unique values, iteration).
    """

    ...


class State(StrEnum):
    """
    Defines the possible lifecycle states for a job within the asyncmq queue system.

    Each state represents a distinct phase a job can be in, from initial creation
    to completion, failure, or being delayed/expired. Inherits from `StrEnum`
    so that state members can be treated directly as their string values.
    """

    WAITING = "waiting"
    """The job is queued and waiting to be processed by a worker."""
    ACTIVE = "active"
    """The job is currently being processed by a worker."""
    COMPLETED = "completed"
    """The job has finished execution successfully."""
    FAILED = "failed"
    """The job failed execution and will not be retried (or retries are exhausted)."""
    DELAYED = "delayed"
    """The job is scheduled to run at a future time."""
    EXPIRED = "expired"
    """The job's time-to-live (TTL) has been exceeded before processing completed."""
    QUEUED = "queued"
    """The job has been queued."""

    def __str__(self) -> str:
        """
        Returns the string value of the enum member.

        This makes the enum member behave like its underlying string value
        when used in string contexts (e.g., print, f-strings).
        """
        # Return the string value associated with the enum member.
        return str(self.value)

    def __repr__(self) -> str:
        """
        Returns a string representation of the enum member for debugging.

        Similar to __str__, this returns the underlying string value.
        """
        # Return the string value for representation.
        return str(self.value)
