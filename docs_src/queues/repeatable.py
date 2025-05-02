queue.add_repeatable(
    task_id="app.cleanup",
    every=3600,       # seconds
    args=None,
    cron=None,
)
