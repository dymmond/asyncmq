from asyncmq.core.event import event_emitter
from asyncmq.logging import logger


def on_complete(payload):
    logger.info(
        f"😃 Job {payload['id']} complete in {payload['timestamps']['finished_at'] - payload['timestamps']['created_at']:.2f}s"
    )


event_emitter.on("job:completed", on_complete)
