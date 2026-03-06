# Flows

`FlowProducer` helps enqueue a set of jobs with dependencies.

```python
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job

producer = FlowProducer(backend)

a = Job(task_id="myapp.tasks.step_a", args=[], kwargs={})
b = Job(task_id="myapp.tasks.step_b", args=[], kwargs={}, depends_on=[a.id])

ids = await producer.add_flow("pipeline", [a, b])
```

## How `add_flow()` Works

`add_flow(queue, jobs)`:

1. serializes jobs (`to_dict()`)
2. builds dependency pairs `(parent_id, child_id)`
3. tries backend `atomic_add_flow(...)`
4. falls back to sequential enqueue + dependency registration

## Atomic vs Fallback

- If backend implements `atomic_add_flow`, flow creation is one backend operation.
- If not, AsyncMQ falls back to a safe sequential path.

In fallback mode, partial creation is possible if an operation fails mid-way. Design tasks to be idempotent.

## Dependency Resolution at Runtime

Workers still enforce dependency checks before execution. A job with unresolved parents is re-delayed briefly and retried later.

## When to Use Flows

Use flows when job ordering/dependency is part of correctness, not just convenience.
