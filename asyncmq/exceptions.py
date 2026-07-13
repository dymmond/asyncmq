class JobCancelled(Exception):
    """Raised when a job is cancelled before or during execution."""


class PayloadTooLarge(ValueError):
    """Raised when a job payload exceeds the configured byte limit."""
