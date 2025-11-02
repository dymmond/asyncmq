from __future__ import annotations

import inspect
from collections.abc import Iterable
from typing import Any, Sequence, TypeAlias

from asyncmq.protocols.lifespan import Lifespan

Hook: TypeAlias = Lifespan | Sequence[Lifespan] | None


def _is_iterable_but_not_callable(value: Any) -> bool:
    return isinstance(value, Iterable) and not callable(value)


def normalize_hooks(hooks: Hook) -> list[Lifespan]:
    """
    Accepts a single hook, a list/tuple of hooks, or None and returns a clean list:
    - filters None
    - preserves order
    - de-duplicates while keeping first occurrence
    """
    if hooks is None:
        return []

    if _is_iterable_but_not_callable(hooks):
        seq = [hook for hook in hooks if hook is not None]  # type: ignore
    else:
        seq = [hooks]

    seen: set[int] = set()
    out: list[Lifespan] = []
    for fn in seq:
        if fn is None:
            continue

        key = id(fn)
        if key in seen:
            continue

        seen.add(key)
        out.append(fn)
    return out


async def run_hooks(hooks: Hook, **kwargs: Any) -> None:
    """
    Run hooks in order. Supports sync or async callables.
    Propagates exceptions (use during startup so failures are visible).
    """
    for fn in normalize_hooks(hooks):
        result = fn(**kwargs)
        if inspect.isawaitable(result):
            await result


async def run_hooks_safely(hooks: Hook, swallow: bool = True, **kwargs: Any) -> None:
    """
    Run hooks in order. If `swallow=True`, suppress exceptions (good for shutdown).
    """
    for fn in normalize_hooks(hooks):
        try:
            result = fn(**kwargs)
            if inspect.isawaitable(result):
                await result
        except Exception:
            if not swallow:
                raise
