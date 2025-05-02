from asyncmq.queues import Queue

Queue(
    name: str,
    backend: BaseBackend | None = None,
    concurrency: int = 3,
    rate_limit: int | None = None,
    rate_interval: float = 1.0,
    scan_interval: float | None = None,
)
