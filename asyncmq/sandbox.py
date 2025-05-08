import inspect
import multiprocessing as mp
import traceback
from typing import Any

import anyio

from asyncmq.conf import monkay
from asyncmq.tasks import TASK_REGISTRY


def _worker_entry(task_id: str, args: list[Any], kwargs: dict[str, Any], out_q: mp.Queue) -> None:
    """
    Entry point for the worker process.

    This function is executed in a separate subprocess to run a specific task
    handler. It retrieves the handler function based on the provided task ID,
    executes it with the given arguments and keyword arguments, and places the
    result or any encountered exception details into the output queue for the
    parent process to retrieve. Handles both synchronous and asynchronous
    handler functions.

    Args:
        task_id: The unique identifier string for the task to be executed.
        args: A list of positional arguments to pass to the task handler.
        kwargs: A dictionary of keyword arguments to pass to the task handler.
        out_q: A multiprocessing Queue object used to send the execution
               result or error information back to the parent process.
    """
    try:
        # Retrieve the task function registered under the given task_id
        func = TASK_REGISTRY[task_id]["func"]

        # Execute the handler function
        if inspect.iscoroutinefunction(func):
            # If the function is a coroutine, run it synchronously using anyio
            result = anyio.run(func, *args, **kwargs)
        else:
            # Otherwise, execute the function directly
            result = func(*args, **kwargs)

        # Put the successful result into the output queue
        out_q.put(("success", result))
    except Exception as e:
        # Catch any exception that occurs during execution
        # Capture the full traceback of the exception
        tb = traceback.format_exc()
        # Prepare a payload containing error details
        payload = {"type": e.__class__.__name__, "message": str(e), "traceback": tb}
        # Put the error payload into the output queue
        out_q.put(("error", payload))


def run_handler(task_id: str, args: list[Any], kwargs: dict[str, Any], timeout: float, fallback: bool = True) -> Any:
    """
    Runs the specified task handler in a sandboxed subprocess with a timeout.

    This function creates a new process to execute the task handler identified
    by `task_id`. It waits for the process to complete within the specified
    `timeout`. If the process exceeds the timeout or encounters an error,
    it handles the situation by either raising an exception or, if `fallback`
    is True and a timeout occurred, executing the handler directly in the
    current process.

    Args:
        task_id: The unique identifier string for the task to be executed.
        args: A list of positional arguments to pass to the task handler.
        kwargs: A dictionary of keyword arguments to pass to the task handler.
        timeout: The maximum number of seconds to wait for the subprocess
                 to complete.
        fallback: If True, execute the task in the current process if the
                  subprocess times out. Defaults to True.

    Returns:
        The result returned by the task handler function upon successful
        execution.

    Raises:
        TimeoutError: If the subprocess execution exceeds the specified timeout
                      and `fallback` is False.
        RuntimeError: If the subprocess fails to return a result or if the
                      subprocess reports an error during execution.
    """
    # Get the multiprocessing context, defaulting to 'fork' if not specified
    ctx = mp.get_context(monkay.settings.sandbox_ctx or "fork")
    # Create a queue for communication between the parent and child processes
    out_q = ctx.Queue()
    # Create a new process targeting _worker_entry with necessary arguments
    proc = ctx.Process(target=_worker_entry, args=(task_id, args, kwargs, out_q))

    try:
        # Start the worker process
        proc.start()
        # Wait for the worker process to complete, with a timeout
        proc.join(timeout)

        # Check if the process is still alive after the join timeout
        if proc.is_alive():
            # If alive, terminate the process
            proc.terminate()
            # Wait for the process to actually terminate
            proc.join()
            # Raise a TimeoutError indicating the task exceeded the limit
            raise TimeoutError(f"Task '{task_id}' exceeded timeout of {timeout} seconds")

        # Check if the output queue is empty after the process finished
        if out_q.empty():
            # If empty, raise a RuntimeError as no response was received
            raise RuntimeError(f"Task '{task_id}' failed without response")

        # Get the status and payload from the output queue
        status, payload = out_q.get()
        # Check the status reported by the worker process
        if status == "success":
            # If success, return the payload (the task result)
            return payload
        else:
            # If status is not success, it's an error. Raise a RuntimeError
            # including the error type, message, and traceback from the payload.
            raise RuntimeError(
                f"Task '{task_id}' error {payload['type']}: {payload['message']}\n" f"{payload['traceback']}"
            )

    except TimeoutError:
        # This block is executed if the initial proc.join(timeout) raises
        # a TimeoutError (which it doesn't, the check after join does).
        # This catch is primarily for the TimeoutError raised explicitly above.
        # If a TimeoutError occurred and fallback is enabled
        if fallback:
            # Retrieve the handler function
            handler = TASK_REGISTRY[task_id]["func"]
            # Run the handler directly in the current process
            if inspect.iscoroutinefunction(handler):
                # If async, use anyio.run
                return anyio.run(handler, *args, **kwargs)
            else:
                # If sync, call directly
                return handler(*args, **kwargs)
        else:
            # If fallback is disabled, re-raise the TimeoutError
            raise
