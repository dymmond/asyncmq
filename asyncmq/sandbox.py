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
    """
    Entry point function for the multiprocessing worker process.

    This function is executed in a separate process. It retrieves the task
    handler from the registry based on the provided task ID, executes the
    handler with the given arguments and keyword arguments, and puts the
    result or any encountered exception details into the output queue.

    Args:
        task_id: The unique identifier of the task to execute.
        args: A tuple of positional arguments to pass to the task handler.
        kwargs: A dictionary of keyword arguments to pass to the task handler.
        out_q: A multiprocessing.Queue used to send the execution result or
               error information back to the parent process.
    """
    try:
        # Retrieve the task metadata from the registry.
        meta = TASK_REGISTRY.get(task_id)
        # Get the actual function handler from the metadata.
        handler = meta["func"] if meta else None
        # If the task ID is not found in the registry, raise a KeyError.
        if handler is None:
            raise KeyError(f"No task registered with id '{task_id}'")

        # Execute the handler function. Check if it's an asynchronous
        # function and run it accordingly using anyio.run if needed.
        if inspect.iscoroutinefunction(handler):
            # Run the asynchronous handler within an anyio event loop.
            result = anyio.run(handler, *args, **kwargs)
        else:
            # Run the synchronous handler directly.
            result = handler(*args, **kwargs)

        # If execution was successful, put a "success" status and the result
        # into the output queue.
        out_q.put(("success", result))
    except Exception as e:
        # If an exception occurs during execution, format the traceback.
        tb = traceback.format_exc()
        # Put an "error" status and a dictionary containing error details
        # (type, message, traceback) into the output queue.
        out_q.put(("error", {"type": e.__class__.__name__, "message": str(e),
                             "traceback": tb}))


def run_handler(
    task_id: str,
    args: Tuple[Any, ...],
    kwargs: dict,
    timeout: float
) -> Any:
    """
    Executes the AsyncMQ task identified by `task_id` in a sandboxed subprocess
    with a specified timeout.

    This function starts a new process to run the task handler. It waits for
    the process to complete, enforcing a timeout. If the process exceeds the
    timeout, it is terminated. It retrieves the execution result or error
    details from a queue shared with the subprocess and raises an appropriate
    exception or returns the result.

    Args:
        task_id: The unique identifier of the task to execute.
        args: A tuple of positional arguments to pass to the task handler.
        kwargs: A dictionary of keyword arguments to pass to the task handler.
        timeout: The maximum duration in seconds the task is allowed to run.

    Returns:
        The result returned by the task handler upon successful execution.

    Raises:
        KeyError: If the task ID is not found in the task registry before
                  starting the process.
        TimeoutError: If the subprocess execution exceeds the specified timeout.
        RuntimeError: If the subprocess fails to return a result via the queue
                      or if the task handler raises an unhandled exception
                      within the subprocess. The message includes details
                      from the subprocess exception.
    """
    # Pre-flight validation: Check if the task ID exists in the registry
    # before attempting to start a process.
    if task_id not in TASK_REGISTRY:
        raise KeyError(f"No task registered with id '{task_id}'")

    # Create a multiprocessing Queue to receive the result or error from
    # the subprocess. The queue size is limited to 1 as we expect only one
    # result/error message.
    out_q: multiprocessing.Queue = multiprocessing.Queue(1)
    # Create a new Process targeting the `_worker_entry` function.
    # Pass the task details and the output queue as arguments.
    # Set daemon=True so the process is killed automatically if the parent
    # process exits.
    proc = multiprocessing.Process(
        target=_worker_entry,
        args=(task_id, args, kwargs, out_q),
        daemon=True,
    )
    # Start the subprocess.
    proc.start()
    # Join the subprocess, waiting for it to complete for up to 'timeout' seconds.
    proc.join(timeout)

    # Check if the process is still alive after the join with timeout.
    if proc.is_alive():
        # If the process is still alive, it means it timed out.
        # Terminate the process.
        proc.terminate()
        # Join again to ensure the process has exited after termination.
        proc.join()
        # Raise a TimeoutError indicating the task exceeded its allowed time.
        raise TimeoutError(f"Task '{task_id}' exceeded timeout of {timeout} seconds")

    # Check if the output queue is empty. If the process finished but didn't
    # put anything in the queue, it's an unexpected failure.
    if out_q.empty():
        # Raise a RuntimeError indicating the task failed without sending
        # a response.
        raise RuntimeError(f"Task '{task_id}' failed without response")

    # Get the result or error payload from the output queue.
    status, payload = out_q.get()
    # Check the status received from the subprocess.
    if status == "success":
        # If status is "success", return the payload (the task's result).
        return payload
    else:
        # If status is "error", the payload contains error details.
        # Raise a RuntimeError including the error type, message, and traceback
        # received from the subprocess.
        # payload is a dict with keys: 'type', 'message', 'traceback'.
        raise RuntimeError(
            f"Task '{task_id}' error {payload['type']}: {payload['message']}\n"
            f"{payload['traceback']}"
        )
