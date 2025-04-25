import asyncio


class EventEmitter:
    def __init__(self):
        self._listeners = {}

    def on(self, event: str, callback):
        self._listeners.setdefault(event, []).append(callback)

    def off(self, event: str, callback):
        if event in self._listeners:
            self._listeners[event] = [
                cb for cb in self._listeners[event] if cb != callback
            ]
            if not self._listeners[event]:
                del self._listeners[event]

    async def emit(self, event: str, data):
        listeners = self._listeners.get(event, [])
        for cb in listeners:
            if asyncio.iscoroutinefunction(cb):
                await cb(data)
            else:
                cb(data)


# Global event emitter
event_emitter = EventEmitter()
