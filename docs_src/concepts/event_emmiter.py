from asyncmq.core.event import event_emitter


def on_complete(payload):
    print(f"Job {payload['id']} completed at {payload['timestamps']['finished_at']}")

event_emitter.on("job:completed", on_complete)
