import pytest

from asyncmq.core.enums import State
from asyncmq.core.event import EventEmitter

pytestmark = pytest.mark.anyio

async def test_event_subscription_and_publish():
    emitter = EventEmitter()
    events = []

    def on_complete(data):
        events.append(f"completed:{data['id']}")

    emitter.on(State.COMPLETED, on_complete)
    await emitter.emit(State.COMPLETED, {"id": "abc123"})

    assert "completed:abc123" in events


async def test_multiple_event_listeners():
    emitter = EventEmitter()
    a, b = [], []

    emitter.on("done", lambda d: a.append(d["val"]))
    emitter.on("done", lambda d: b.append(d["val"]))
    await emitter.emit("done", {"val": 5})

    assert a == [5]
    assert b == [5]


async def test_event_emit_no_listeners():
    emitter = EventEmitter()
    await emitter.emit("unknown", {"x": 1})  # Should not crash
    assert True


async def test_event_remove_listener():
    emitter = EventEmitter()
    result = []

    def handler(data):
        result.append(data["msg"])

    emitter.on("ping", handler)
    emitter.off("ping", handler)
    await emitter.emit("ping", {"msg": "pong"})
    assert not result


async def test_async_event_handler():
    emitter = EventEmitter()
    result = []

    async def handler(data):
        result.append(data["key"])

    emitter.on("trigger", handler)
    await emitter.emit("trigger", {"key": "val"})
    assert result == ["val"]


async def test_emit_with_multiple_event_types():
    emitter = EventEmitter()
    res = []

    emitter.on("a", lambda d: res.append("A"))
    emitter.on("b", lambda d: res.append("B"))
    await emitter.emit("a", {})
    await emitter.emit("b", {})

    assert res == ["A", "B"]


async def test_same_handler_multiple_events():
    emitter = EventEmitter()
    logs = []

    def handler(d):
        logs.append(f"{d['tag']}")

    emitter.on("x", handler)
    emitter.on("y", handler)
    await emitter.emit("x", {"tag": "x-event"})
    await emitter.emit("y", {"tag": "y-event"})

    assert logs == ["x-event", "y-event"]


async def test_off_removes_only_targeted_handler():
    emitter = EventEmitter()
    result = []

    def one(d): result.append("one")
    def two(d): result.append("two")

    emitter.on("e", one)
    emitter.on("e", two)
    emitter.off("e", one)
    await emitter.emit("e", {})

    assert result == ["two"]


async def test_emit_with_empty_data():
    emitter = EventEmitter()
    result = []
    emitter.on("blank", lambda _: result.append("ok"))
    await emitter.emit("blank", {})
    assert result == ["ok"]


async def test_handler_modifies_external_state():
    emitter = EventEmitter()
    state = {"count": 0}

    def inc(_): state["count"] += 1
    emitter.on("tick", inc)
    await emitter.emit("tick", {})
    await emitter.emit("tick", {})

    assert state["count"] == 2


async def test_emit_does_not_affect_other_events():
    emitter = EventEmitter()
    out = []
    emitter.on("x", lambda _: out.append("x"))
    emitter.on("y", lambda _: out.append("y"))
    await emitter.emit("x", {})
    assert "y" not in out
