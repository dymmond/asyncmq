import inspect
from typing import Any, Callable, Coroutine, Dict, List, Union

import anyio

# A callback can be sync or async
Callback = Union[
    Callable[[Any], Any],
    Callable[[Any], Coroutine[Any, Any, Any]]
]


class EventEmitter:
    def __init__(self):
        # maps event name to list of callbacks
        self._listeners: Dict[str, List[Callback]] = {}

    def on(self, event: str, callback: Callback) -> None:
        """Register a listener for an event."""
        self._listeners.setdefault(event, []).append(callback)

    def off(self, event: str, callback: Callback) -> None:
        """Unregister a listener for an event."""
        if event not in self._listeners:
            return
        self._listeners[event] = [cb for cb in self._listeners[event] if cb != callback]
        if not self._listeners[event]:
            del self._listeners[event]

    async def emit(self, event: str, data: Any) -> None:
        """
        Emit `data` to all listeners of `event`.
        Async listeners run in the native loop; sync listeners run in a thread.
        All handlers execute concurrently.
        """
        listeners = list(self._listeners.get(event, []))
        if not listeners:
            return

        async with anyio.create_task_group() as tg:
            for cb in listeners:
                if inspect.iscoroutinefunction(cb):
                    tg.start_soon(cb, data)
                else:
                    # run sync callback in a thread
                    tg.start_soon(anyio.to_thread.run_sync, cb, data)


# Global singleton
event_emitter = EventEmitter()
