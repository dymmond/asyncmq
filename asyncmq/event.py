import inspect
from typing import Any, Callable, Coroutine

import anyio

# Define a type alias for callbacks that can be either synchronous or asynchronous.
# A synchronous callback takes Any and returns Any.
# An asynchronous callback takes Any and returns an Awaitable that resolves to Any.
Callback = Callable[[Any], Any] | Callable[[Any], Coroutine[Any, Any, Any]]


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
        Each value is a list of registered callback functions for that event.
        """
        # Dictionary to store registered callbacks, keyed by event name.
        self._listeners: dict[str, list[Callback]] = {}

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
        # Add the callback to the list of listeners for the given event.
        # Use setdefault to create an empty list if the event is not yet present.
        self._listeners.setdefault(event, []).append(callback)

    def off(self, event: str, callback: Callback) -> None:
        """
        Unregisters a specific callback function from a specific event.

        If the `callback` is registered for the given `event`, it is removed
        from the list of listeners for that event. If the event had no other
        listeners remaining after removal, the event entry is removed from
        the internal dictionary.

        Args:
            event: The name of the event (string) from which to unregister
                   the callback.
            callback: The specific callable function to unregister.
        """
        # If the event does not exist, there are no listeners to remove.
        if event not in self._listeners:
            return

        # Filter out the specific callback from the list of listeners for the event.
        self._listeners[event] = [cb for cb in self._listeners[event] if cb != callback]

        # If the list of listeners for this event becomes empty, remove the event key.
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
        # Get the list of listeners for the event, defaulting to an empty list.
        # Create a copy to avoid issues if listeners modify the list during emission.
        listeners: list[Callback] = list(self._listeners.get(event, []))

        # If there are no listeners for this event, return immediately.
        if not listeners:
            return

        # Create a TaskGroup to run callbacks concurrently.
        async with anyio.create_task_group() as tg:
            # Iterate through each registered callback.
            for cb in listeners:
                # Check if the callback is an asynchronous function.
                if inspect.iscoroutinefunction(cb):
                    # If it's async, start it directly as a new task in the TaskGroup.
                    tg.start_soon(cb, data)
                else:
                    # If it's synchronous, run it in a thread to avoid blocking
                    # the main event loop, also started as a task.
                    tg.start_soon(anyio.to_thread.run_sync, cb, data)


# The singleton instance of the `EventEmitter` class, providing a global event bus
# for the asyncmq system. Components can register listeners or emit events using
# this instance.
event_emitter: EventEmitter = EventEmitter()
