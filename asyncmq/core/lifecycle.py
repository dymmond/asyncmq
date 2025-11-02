from __future__ import annotations

import inspect
from collections.abc import Iterable, Sequence
from typing import Any, TypeAlias

from asyncmq.protocols.lifespan import Lifespan

Hook: TypeAlias = Lifespan | Sequence[Lifespan] | None


def _is_iterable_but_not_callable(value: Any) -> bool:
    """
    Checks if a value is iterable but not a callable function (e.g., list, tuple).
    This prevents iterating over a single `Lifespan` function if it's passed directly.
    """
    # Excludes strings, which are technically iterable but treated as scalars here.
    return isinstance(value, Iterable) and not callable(value) and not isinstance(value, str)


def normalize_hooks(hooks: Hook) -> list[Lifespan]:
    """
    Accepts a single hook, a sequence of hooks, or None and returns a clean, ordered list of hooks.

    The normalization process:
    - Filters out any `None` values.
    - Preserves the original order of the hooks.
    - De-duplicates hooks based on object identity (function reference), keeping the first occurrence.

    Args:
        hooks: The input hook(s), which can be a single hook, a list/tuple of hooks, or None.

    Returns:
        A cleaned, ordered list of unique `Lifespan` callables.
    """
    if hooks is None:
        return []

    # Convert to a flat list, handling the case where hooks is a single hook or a sequence.
    if _is_iterable_but_not_callable(hooks):
        # Cast hooks to a sequence for iteration
        seq: list[Lifespan | None] = [hook for hook in hooks if hook is not None]  # type: ignore
    else:
        # Hooks is a single Lifespan or None (already handled above)
        seq = [hooks]  # type: ignore

    # De-duplicate based on object identity (the function object itself)
    seen_ids: set[int] = set()
    out: list[Lifespan] = []

    # Use object identity (id()) for stable and reliable function de-duplication
    for fn in seq:
        # Since we pre-filtered None above, fn should be a Lifespan
        if fn is None:
            continue

        key: int = id(fn)
        if key not in seen_ids:
            seen_ids.add(key)
            out.append(fn)

    return out


async def run_hooks(hooks: Hook, **kwargs: Any) -> None:
    """
    Executes a sequence of lifespan hooks synchronously or asynchronously.

    This runner is suitable for **startup** hooks because it **propagates exceptions**
    upwards, making critical failures immediately visible.

    Args:
        hooks: The hook(s) to run (single hook, sequence, or None).
        **kwargs: Keyword arguments to pass to each hook function.
    """
    for fn in normalize_hooks(hooks):
        result: Any = fn(**kwargs)
        if inspect.isawaitable(result):
            await result


async def run_hooks_safely(hooks: Hook, swallow: bool = True, **kwargs: Any) -> None:
    """
    Executes a sequence of lifespan hooks, optionally suppressing exceptions.

    This runner is ideal for **shutdown** hooks. By default, it **swallows exceptions**
    to ensure that a failure in one cleanup routine doesn't prevent subsequent routines
    from running.

    Args:
        hooks: The hook(s) to run (single hook, sequence, or None).
        swallow: If `True` (default), exceptions raised by hooks are ignored.
                 If `False`, exceptions are propagated (similar to `run_hooks`).
        **kwargs: Keyword arguments to pass to each hook function.
    """
    for fn in normalize_hooks(hooks):
        try:
            result: Any = fn(**kwargs)
            if inspect.isawaitable(result):
                await result
        except Exception:
            if not swallow:
                raise
