import inspect
import multiprocessing as mp
import traceback

import anyio

from asyncmq.conf import settings
from asyncmq.tasks import TASK_REGISTRY


def _worker_entry(task_id, args, kwargs, out_q):
    """
    Entry point run in subprocess: executes the handler and puts result or error into queue.
    """
    try:
        func = TASK_REGISTRY[task_id]["func"]
        # call handler
        if inspect.iscoroutinefunction(func):
            # run async handlers synchronously via anyio
            result = anyio.run(func, *args, **kwargs)
        else:
            result = func(*args, **kwargs)
        out_q.put(("success", result))
    except Exception as e:
        # capture traceback and send error payload
        tb = traceback.format_exc()
        payload = {"type": e.__class__.__name__, "message": str(e), "traceback": tb}
        out_q.put(("error", payload))




def _worker_entry(task_id, args, kwargs, out_q):
    """
    Entry point run in subprocess: executes the handler and puts result or error into queue.
    """
    try:
        func = TASK_REGISTRY[task_id]["func"]
        # call handler
        if inspect.iscoroutinefunction(func):
            # run async handlers synchronously via anyio
            result = anyio.run(func, *args, **kwargs)
        else:
            result = func(*args, **kwargs)
        out_q.put(("success", result))
    except Exception as e:
        # capture traceback and send error payload
        tb = traceback.format_exc()
        payload = {"type": e.__class__.__name__, "message": str(e), "traceback": tb}
        out_q.put(("error", payload))


def run_handler(task_id: str, args: list, kwargs: dict, timeout: float, fallback: bool = True):
    """
    Runs the specified task in a sandboxed subprocess with a timeout.

    If the subprocess times out and fallback=True, it will execute the task in-process as a backup.
    """
    # Prepare subprocess context and communication
    ctx = mp.get_context(settings.sandbox_ctx or 'fork')
    out_q = ctx.Queue()
    proc = ctx.Process(target=_worker_entry, args=(task_id, args, kwargs, out_q))

    try:
        proc.start()
        proc.join(timeout)

        if proc.is_alive():
            proc.terminate()
            proc.join()
            raise TimeoutError(
                f"Task '{task_id}' exceeded timeout of {timeout} seconds"
            )

        if out_q.empty():
            raise RuntimeError(f"Task '{task_id}' failed without response")

        status, payload = out_q.get()
        if status == "success":
            return payload
        else:
            raise RuntimeError(
                f"Task '{task_id}' error {payload['type']}: {payload['message']}\n"
                f"{payload['traceback']}"
            )

    except TimeoutError:
        if fallback:
            handler = TASK_REGISTRY[task_id]['func']
            if inspect.iscoroutinefunction(handler):
                return anyio.run(handler, *args, **kwargs)
            else:
                return handler(*args, **kwargs)
        else:
            raise
