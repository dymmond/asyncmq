from collections import defaultdict
from typing import Any, Callable, Dict, List


class EventEmitter:
    def __init__(self):
        self._handlers: Dict[str, List[Callable[[Any], None]]] = defaultdict(list)

    def on(self, event: str, handler: Callable[[Any], None]):
        self._handlers[event].append(handler)

    async def emit(self, event: str, payload: Any):
        for handler in self._handlers[event]:
            await handler(payload)

# Global event emitter
event_emitter = EventEmitter()
