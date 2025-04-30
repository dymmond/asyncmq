# File: asyncmq/sandbox.py

import inspect
import multiprocessing
import traceback
from typing import Any, Tuple

import anyio

from asyncmq.tasks import TASK_REGISTRY


def _worker_entry(
    task_id: str,
    args: Tuple[Any, ...],
    kwargs: dict,
    out_q: multiprocessing.Queue
):
    try:
        meta = TASK_REGISTRY.get(task_id)
        handler = meta["func"] if meta else None
        if handler is None:
            raise KeyError(f"No task registered with id '{task_id}'")
        # Execute synchronous or async handler
        if inspect.iscoroutinefunction(handler):
            result = anyio.run(handler, *args, **kwargs)
        else:
            result = handler(*args, **kwargs)
        out_q.put(("success", result))
    except Exception as e:
        tb = traceback.format_exc()
        out_q.put(("error", {"type": e.__class__.__name__, "message": str(e), "traceback": tb}))


def run_handler(
    task_id: str,
    args: Tuple[Any, ...],
    kwargs: dict,
    timeout: float
) -> Any:
    """
    Execute the AsyncMQ task identified by `task_id` in a sandboxed subprocess.
    Raises KeyError if the task is not registered, TimeoutError if the handler exceeds `timeout`,
    or RuntimeError on handler error.
    """
    # Pre-flight validation
    if task_id not in TASK_REGISTRY:
        raise KeyError(f"No task registered with id '{task_id}'")

    out_q: multiprocessing.Queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(
        target=_worker_entry,
        args=(task_id, args, kwargs, out_q),
        daemon=True,
    )
    proc.start()
    proc.join(timeout)

    if proc.is_alive():
        proc.terminate()
        proc.join()
        raise TimeoutError(f"Task '{task_id}' exceeded timeout of {timeout} seconds")

    if out_q.empty():
        raise RuntimeError(f"Task '{task_id}' failed without response")

    status, payload = out_q.get()
    if status == "success":
        return payload
    else:
        # payload is a dict with type, message, traceback
        raise RuntimeError(
            f"Task '{task_id}' error {payload['type']}: {payload['message']}\n{payload['traceback']}"
        )
