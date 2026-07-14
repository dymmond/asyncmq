from __future__ import annotations

from collections.abc import Callable
from typing import Any

from asyncmq.exceptions import PayloadTooLarge


def job_payload_size_bytes(payload: dict[str, Any], dumps: Callable[[Any], str]) -> int:
    """
    Return the UTF-8 encoded size of a serialized job payload.

    The producer path measures the same JSON representation that backends use
    for storage, which keeps the limit independent of Python object overhead.
    """
    encoded = dumps(payload)
    if isinstance(encoded, bytes):
        return len(encoded)
    if not isinstance(encoded, str):
        raise TypeError("settings.json_dumps must return str or bytes.")
    return len(encoded.encode("utf-8"))


def validate_job_payload_size(
    payload: dict[str, Any],
    *,
    max_bytes: int | None,
    dumps: Callable[[Any], str],
) -> None:
    """
    Reject payloads that exceed the configured JSON-encoded byte limit.

    Args:
        payload: Normalized job payload.
        max_bytes: Maximum allowed encoded payload size. ``None`` disables the
            guard.
        dumps: JSON serializer used by the current settings object.
    """
    if max_bytes is None:
        return
    if max_bytes < 0:
        raise ValueError("max_job_payload_bytes must be non-negative or None.")

    size = job_payload_size_bytes(payload, dumps)
    if size > max_bytes:
        raise PayloadTooLarge(f"Job payload is {size} bytes, exceeding max_job_payload_bytes={max_bytes}.")
