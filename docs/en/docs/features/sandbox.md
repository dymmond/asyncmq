# Sandboxing & Safe Execution

Sandboxing in AsyncMQ lets you run potentially untrusted or CPU‑heavy tasks in isolated subprocesses,
protecting your main worker process from crashes, leaks, or endless loops. This guide dives into:

1. **Overview & Benefits**
2. **Key Components**: `_worker_entry` & `run_handler`
3. **Configuration via `Settings`**
4. **Usage Examples**
5. **Advanced Patterns**
6. **Testing & Best Practices**
7. **Common Pitfalls & FAQs**

---

## 1. Why Sandbox?

* 🔒 **Isolation**: Crashes, memory leaks, or segfaults in task code stay in the subprocess—not your main worker.
* ⏱️ **Timeout Control**: Enforce maximum execution times to avoid runaway tasks.
* 🧹 **Cleanup**: Subprocess exit reclaims resources (file handles, threads).

!!! Analogy
    Think of sandboxing as hiring a Pyro‑proof robot to do risky chemistry experiments while you sip coffee safely nearby. ☕🧪

---

## 2. Key Components

### 2.1. `run_handler`

```python
def run_handler(
    task_id: str, args: list[Any], kwargs: dict[str, Any],
    timeout: float, fallback: bool = True
) -> Any:
    ...
```

* **Timeout**: Kills and reports if the task exceeds the limit.
* **Fallback**: Optionally runs the task inline if sandbox fails or times out.
* **Process Context**: Controlled by `settings.sandbox_ctx` (`fork`, `spawn`, etc.).

---

## 3. Configuration via `Settings`

Adjust sandbox behavior in your custom `Settings` class:

```python
from asyncmq import Settings as BaseSettings

class Settings(BaseSettings):
    sandbox_enabled: bool = True                   # Turn sandbox on
    sandbox_default_timeout: float = 30.0           # Seconds before kill
    sandbox_ctx: str = 'spawn'                    # Multiprocessing context
```

AsyncMQ checks `settings.sandbox_enabled` before invoking `run_handler` inside worker loops or `process_job`.

---

## 4. Usage Examples

### 4.1. Protecting a Legacy Function

```python
@task(queue='legacy', progress=False)
def old_heavy(data):  # blocks, raises, or leaks
    ...
# In worker or handler logic:
if settings.sandbox_enabled:
    result = run_handler(old_heavy.task_id, [data], {}, settings.sandbox_default_timeout)
else:
    result = old_heavy(data)
```

### 4.2. Custom Timeout per-Job

```python
# Override default for this specific job
await some_task.enqueue(..., backend=backend, timeout=5.0)
# In handler routing code, pass timeout through to run_handler.
```

---

## 5. Advanced Patterns

* **Chained Sandboxes**: For dependency chains, isolate each step to prevent cross-contamination.
* **Resource Limits**: Combine sandbox with OS-level controls (cgroups, ulimits) for CPU/Memory caps.
* **Parallel Sandboxes**: Spawn multiple sandboxes concurrently for CPU‑bound tasks; beware of process explosion.

---

## 6. Testing & Best Practices

* **Unit Test Safe Paths**: Ensure `_worker_entry` correctly serializes success and failure.
* **Simulate Timeouts**: Write a dummy task that sleeps > timeout to test branch.
* **Fallback Validation**: Test both `fallback=True` and `False` behaviors.

> 🧪 **Tip:** In tests, reduce `sandbox_default_timeout` to milliseconds for speed.

---

## 7. Common Pitfalls & FAQs

* **Zombie Processes**: Ensure `.join()` after `.terminate()` to avoid zombies.
* **Queue Full**: If `out_q` blocks (full), parent may hang—use a large enough buffer or non-blocking read.
* **Pickle Failures**: Functions or arguments must be pickleable; lambdas or local functions won't work.
* **Inconsistent Context**: Using `fork` on Mac can duplicate file descriptors—use `spawn` where unpredictable behavior occurs.

> ❓ **FAQ:** *What if my task needs access to globals or modules loaded in the parent?*
> The subprocess imports fresh copies; ensure necessary imports at top-level or use a wrapper module.

---

With sandboxing in place, your AsyncMQ workers gain superpowers—robust isolation, enforced timeouts, and graceful
fallbacks, making your background processing bulletproof (or at least blast-resistant)! 🛡️
