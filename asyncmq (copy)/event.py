import inspect
from typing import Any, Callable, Coroutine, Dict, List, Union

import anyio

# Define a type alias for callbacks that can be either synchronous or asynchronous.
Callback = Union[
    Callable[[Any], Any],
    Callable[[Any], Coroutine[Any, Any, Any]]
]
"""
Type alias representing a function that can be registered as an event listener.
A callback can be either a synchronous function or a coroutine function.
Both are expected to accept a single argument (the event data) of type `Any`.
"""


class EventEmitter:
    """
    A simple publish-subscribe mechanism for managing and emitting events.

    This class allows registering callback functions (listeners) for specific
    event names. When an event is emitted with associated data, all registered
    listeners for that event are invoked. It handles both synchronous and
    asynchronous listeners using AnyIO's task group and thread offloading.
    """
    def __init__(self) -> None:
        """
        Initializes a new EventEmitter instance.

        Creates an empty dictionary to store listeners, keyed by event name.
        """
        # A dictionary where keys are event names (string) and values are
        # lists of registered callback functions (Callback type).
        self._listeners: Dict[str, List[Callback]] = {}

    def on(self, event: str, callback: Callback) -> None:
        """
        Registers a callback function as a listener for a specific event.

        When the specified `event` is emitted, the provided `callback` function
        will be invoked with the event data. A single callback can be registered
        for multiple events, and multiple callbacks can be registered for the
        same event.

        Args:
            event: The name of the event (string) to listen for.
            callback: The callable function (synchronous or asynchronous) to
                      be executed when the event is emitted.
        """
        # Use setdefault to ensure the event key exists with an empty list if not present,
        # then append the callback to the list of listeners for this event.
        self._listeners.setdefault(event, []).append(callback)

    def off(self, event: str, callback: Callback) -> None:
        """
        Unregisters a specific callback function from a specific event.

        If the `callback` is registered for the given `event`, it is removed
        from the list of listeners for that event. If the event had no other
        listeners remaining after removal, the event entry is removed from
        the internal dictionary.

        Args:
            event: The name of the event (string) from which to unregister the callback.
            callback: The specific callable function to unregister.
        """
        # Check if the event exists in the listeners dictionary.
        if event not in self._listeners:
            return # If the event doesn't exist, there's nothing to unregister.

        # Filter the list of listeners for the event, keeping only callbacks that are not
        # the one being unregistered.
        self._listeners[event] = [cb for cb in self._listeners[event] if cb != callback]

        # If the list of listeners for this event is now empty, remove the event entry
        # from the dictionary to clean up.
        if not self._listeners[event]:
            del self._listeners[event]

    async def emit(self, event: str, data: Any) -> None:
        """
        Emits an event, triggering all registered listeners for that event.

        All callback functions registered for the specified `event` will be
        executed concurrently within an AnyIO TaskGroup. Asynchronous callbacks
        (coroutine functions) are started directly as tasks. Synchronous callbacks
        are automatically run in a thread from AnyIO's thread pool to avoid
        blocking the event loop. The `data` argument is passed to each listener.

        Args:
            event: The name of the event (string) to emit.
            data: The data associated with the event, which will be passed
                  as an argument to the listener callbacks.
        """
        # Get the list of listeners for the event. Create a copy to avoid issues
        # if a listener modifies the _listeners dictionary during iteration.
        listeners: List[Callback] = list(self._listeners.get(event, []))

        # If there are no listeners for this event, simply return.
        if not listeners:
            return

        # Create an AnyIO TaskGroup to manage the concurrent execution of listeners.
        async with anyio.create_task_group() as tg:
            # Iterate through each registered callback for the event.
            for cb in listeners:
                # Check if the callback is an asynchronous function (coroutine function).
                if inspect.iscoroutinefunction(cb):
                    # If it's async, start it directly as a new task in the TaskGroup.
                    tg.start_soon(cb, data)
                else:
                    # If it's a synchronous function, run it in a thread using AnyIO.
                    # This prevents synchronous code from blocking the event loop.
                    # anyio.to_thread.run_sync takes the sync function, its arguments,
                    # and runs it in a worker thread. tg.start_soon runs this thread execution task.
                    tg.start_soon(anyio.to_thread.run_sync, cb, data)


# Global singleton instance of the EventEmitter.
event_emitter: EventEmitter = EventEmitter()
"""
The singleton instance of the `EventEmitter` class, providing a global event bus
for the asyncmq system. Components can register listeners or emit events using
this instance.
"""
