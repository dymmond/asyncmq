# Sandbox Execution

AsyncMQ can execute handlers in a subprocess sandbox.

## Enable It

```python
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    sandbox_enabled = True
    sandbox_default_timeout = 30.0
    sandbox_ctx = "spawn"  # or "fork" depending platform/runtime needs
```

When enabled, worker runtime calls `sandbox.run_handler(...)` instead of executing the handler directly.

## Behavior

`run_handler(task_id, args, kwargs, timeout, fallback=True)`:
- executes task in child process
- waits up to `timeout`
- on timeout, terminates child
- if `fallback=True`, retries execution in current process

## Tradeoffs

Pros:

- isolates crashes from main worker process
- hard execution timeout boundary

Cons:

- extra process startup overhead
- task args/results must be process-serializable

## Practical Notes

- Prefer `spawn` in environments where `fork` semantics are problematic.
- Keep timeout realistic for your workload.
- For performance-critical short jobs, benchmark sandbox overhead before enabling globally.
