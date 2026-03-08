from __future__ import annotations


def validate_custom_job_id(job_id: str) -> None:
    """
    Validate a user-supplied custom job identifier against BullMQ-compatible rules.

    AsyncMQ uses free-form string identifiers for jobs, but producers may also
    provide an explicit ``job_id`` to make enqueue operations idempotent. BullMQ
    applies two important constraints that are portable across backends and worth
    preserving here:

    - numeric-only custom ids are rejected so they cannot be confused with
      auto-generated counters used by some implementations;
    - the ``:`` separator is rejected because several backends use it in their
      own composite storage keys and schedule identifiers.

    Args:
        job_id: The custom identifier requested by the producer.

    Raises:
        TypeError: If ``job_id`` is not a string.
        ValueError: If the identifier is empty, numeric-only, or contains ``:``.
    """
    if not isinstance(job_id, str):
        raise TypeError("Custom job_id must be a string.")
    if not job_id:
        raise ValueError("Custom job_id cannot be empty.")
    if job_id.isdigit():
        raise ValueError("Custom job_id cannot be numeric-only.")
    if ":" in job_id:
        raise ValueError("Custom job_id cannot contain ':'.")
