from asyncmq.core.event import event_emitter


def on_progress(payload):
    print(f"Report {payload['id']} is {payload['progress']*100:.1f}% done")

event_emitter.on("job:progress", on_progress)
