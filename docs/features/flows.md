# Flows & the `FlowProducer`

Flows let you treat multiple interdependent jobs as a single unit, perfect for orchestrating pipelines,
DAGs, or any scenario where one task must wait for another.

The `FlowProducer` class handles enqueuing jobs and wiring up their dependencies, using atomic operations 
when supported or falling back to safe sequential logic.

---

## 1. What Is `FlowProducer`?

The `FlowProducer` (in `asyncmq.flow.`) is your high-level API for:

* **Atomic or safe enqueue** of a graph of jobs
* **Automatic dependency registration** so downstream jobs wait for upstream ones
* **Backend-optimized** operations: uses `atomic_add_flow` when available, else a robust fallback

!!! Metaphor
    Imagine you’re casting a spell—`FlowProducer` is your wand, aligning all the magical runes (jobs) in the
    right order so the incantation executes flawlessly.

---

## 2. Core API

```python
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job

producer = FlowProducer(backend)
job_a = Job(task_id="app.step_a", args=[...])
job_b = Job(task_id="app.step_b", args=[...], depends_on=[job_a.id])
job_c = Job(task_id="app.step_c", args=[...], depends_on=[job_a.id, job_b.id])

ids = await producer.add_flow("pipeline", [job_a, job_b, job_c])
print(ids)  # [job_a.id, job_b.id, job_c.id]
```

### `add_flow(queue: str, jobs: List[Job]) -> List[str]`

* **`queue`**: target queue for all jobs
* **`jobs`**: list of `Job` instances, each may have `depends_on`
* **Returns**: list of job IDs in order enqueued

Behind the scenes, `FlowProducer`:

1. **Builds payloads**: serializes each `Job` via `to_dict()`
2. **Extracts dependencies**: collects `(parent_id, child_id)` links
3. **Attempts atomic enqueue**: calls `backend.atomic_add_flow(queue, payloads, deps)`
4. **Fallback**: if atomic unsupported, enqueues each job then registers dependencies via `add_dependencies()`

---

## 3. Atomic vs Fallback

| Feature          | Atomic (`atomic_add_flow`)      | Fallback (sequential)      |
| ---------------- | ------------------------------- | -------------------------- |
| Consistency      | All-or-nothing enqueue & link   | Jobs may appear one-by-one |
| Dependency Reg’n | Handled server-side in one call | Multiple calls per job     |
| Performance      | Fast & network-efficient        | More round-trips           |
| Backend Support  | Requires custom implementation  | Works on any `BaseBackend` |

**Pro Tip:** If your backend supports `atomic_add_flow`, you enjoy transaction-like guarantees, no partial graphs!
Otherwise, rely on the fallback, which is rock-solid even if slower.

---

## 4. Advanced Usage

### 4.1. Complex DAGs

```python
# Define a diamond-shaped flow
start = Job(task_id="app.start", args=[])
branch1 = Job(task_id="app.process_a", depends_on=[start.id])
branch2 = Job(task_id="app.process_b", depends_on=[start.id])
join = Job(task_id="app.finalize", depends_on=[branch1.id, branch2.id])

await producer.add_flow("default", [start, branch1, branch2, join])
```

### 4.2. Mixed Dependencies & Repeatables

Flows can include repeatable definitions by setting `repeat_every` on a `Job`, but remember the scheduler will re-enqueue those based on your queue’s `scan_interval`.

### 4.3. Custom Dependency Logic

Override the dependency adder by assigning `producer._add_dependencies` to your own coroutine:

```python
async def my_add_deps(backend, queue, job):
    # Custom logic (e.g., logging, auditing)
    await backend.add_dependencies(queue, job.to_dict())

producer = FlowProducer()
producer._add_dependencies = my_add_deps
```

---

## 5. Error Handling & Retries

* **Atomic Path**: if `atomic_add_flow` throws an error, no jobs are enqueued. Handle exceptions to retry the entire flow.
* **Fallback Path**: if enqueue of job N fails, jobs 1..N-1 are already in queue—consider cleanup or idempotent tasks.

> ⚠️ **Warning:** In fallback mode, partial flows can exist. Ensure your tasks handle idempotency or catch exceptions to rollback if needed.

---

## 6. Testing & Best Practices

* **Unit Tests**: Mock `backend.atomic_add_flow` to simulate success/failure paths.
* **Integration Tests**: Use an `InMemoryBackend` to verify dependencies truly prevent premature runs.
* **Idempotency**: Design tasks to be safe if enqueued multiple times (e.g., use upserts).

!!! Tip
    After `add_flow`, assert that `backend.list_jobs("waiting")` shows jobs in the correct dependency order.

---

## 7. Common Pitfalls & FAQs

* **Forgetting `depends_on`**: Missing links means no enforced order—jobs may jostle unpredictably.
* **Circular Dependencies**: `FlowProducer` does not detect cycles—ensure your DAG is acyclic to avoid deadlocks.
* **Huge Flows**: Very large flows in fallback mode can be slow—optimize by batching or using atomic backend.

**FAQ:** *Can I update a flow after creation?*

No—flows are one-off enqueue operations. For dynamic graphs, use `FlowProducer` per run or manage dependencies in separate flows.

---

With `FlowProducer`, you can orchestrate robust job graphs effortlessly, whether you’re building ETL pipelines,
CI/CD workflows, or any multi-step background process.
