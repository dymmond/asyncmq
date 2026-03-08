from __future__ import annotations

import inspect
from typing import Any

import anyio


async def acquire_backend_lock(lock: Any) -> bool:
    """
    Acquire a backend lock using the most capable interface it exposes.

    AsyncMQ backends use a mix of AnyIO locks, Redis locks, and custom lock
    wrappers. This helper normalizes the common acquisition path so queue
    producers and schedulers can serialize critical sections without knowing
    each backend's lock API.

    Args:
        lock: The backend-native lock object returned by ``create_lock``.

    Returns:
        ``True`` when the lock was acquired. Backends that return ``None`` from
        ``acquire`` are also treated as success because AnyIO locks do not
        return a boolean.
    """
    acquire = getattr(lock, "acquire", None)
    if acquire is None:
        return True

    result = acquire()
    if inspect.isawaitable(result):
        result = await result
    return True if result is None else bool(result)


async def try_acquire_backend_lock(lock: Any) -> bool:
    """
    Attempt a non-blocking lock acquisition when the backend supports it.

    This helper is used by the repeatable scheduler. If a backend cannot
    provide a non-blocking attempt, AsyncMQ falls back to a blocking acquire
    so correctness wins over duplicate scheduling.

    Args:
        lock: The backend-native lock object returned by ``create_lock``.

    Returns:
        ``True`` if the lock was acquired, otherwise ``False``.
    """
    try_acquire = getattr(lock, "try_acquire", None)
    if callable(try_acquire):
        result = try_acquire()
        if inspect.isawaitable(result):
            result = await result
        return True if result is None else bool(result)

    acquire_nowait = getattr(lock, "acquire_nowait", None)
    if callable(acquire_nowait):
        try:
            result = acquire_nowait()
        except anyio.WouldBlock:
            return False
        return True if result is None else bool(result)

    acquire = getattr(lock, "acquire", None)
    if acquire is None:
        return True

    signature = inspect.signature(acquire)
    if "blocking" in signature.parameters:
        result = acquire(blocking=False)
        if inspect.isawaitable(result):
            result = await result
        return True if result is None else bool(result)

    return await acquire_backend_lock(lock)


async def release_backend_lock(lock: Any) -> None:
    """
    Release a backend lock if it exposes a release method.

    Args:
        lock: The backend-native lock object returned by ``create_lock``.
    """
    release = getattr(lock, "release", None)
    if release is None:
        return

    result = release()
    if inspect.isawaitable(result):
        await result
