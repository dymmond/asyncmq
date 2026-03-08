# Flows

Flows model job dependencies. They let you describe "run B after A" or
"run D after B and C" without moving orchestration logic into ad hoc task code.

AsyncMQ's flow API is centered on `FlowProducer` plus job-level
`depends_on` metadata.

## What Problem Flows Solve

Use flows when correctness depends on ordering, not just convenience.

Typical cases:

- ETL pipelines
- fan-out / fan-in workloads
- document generation after several preprocessing steps
- multi-stage webhooks where later tasks should not run until earlier ones finish

If independent jobs can safely run in any order, do not use flows just for
organization. Keep them separate and let the queue maximize throughput.

## Basic Example

```python
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job

producer = FlowProducer(backend)

extract = Job(task_id="etl.extract", args=[], kwargs={})
transform = Job(task_id="etl.transform", args=[], kwargs={}, depends_on=[extract.id])
load = Job(task_id="etl.load", args=[], kwargs={}, depends_on=[transform.id])

ids = await producer.add_flow("etl", [extract, transform, load])
```

## Fan-Out / Fan-In Example

```python
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job

producer = FlowProducer(backend)

ingest = Job(task_id="media.ingest", args=["asset-42"], kwargs={})
thumbnail = Job(task_id="media.thumbnail", args=["asset-42"], kwargs={}, depends_on=[ingest.id])
transcode = Job(task_id="media.transcode", args=["asset-42"], kwargs={}, depends_on=[ingest.id])
publish = Job(
    task_id="media.publish",
    args=["asset-42"],
    kwargs={},
    depends_on=[thumbnail.id, transcode.id],
)

await producer.add_flow("media", [ingest, thumbnail, transcode, publish])
```

Here:

- `thumbnail` and `transcode` wait for `ingest`
- `publish` waits for both branches

## How `add_flow()` Works

`FlowProducer.add_flow(queue, jobs)` does four things:

1. serializes every `Job` into the durable payload shape
2. derives dependency edges as `(parent_id, child_id)` pairs
3. tries backend-native `atomic_add_flow(...)`
4. falls back to sequential enqueue plus dependency registration when the backend
   does not support an atomic flow write

That gives AsyncMQ one API surface across several backend capabilities.

## Atomic vs Fallback Behavior

If the backend implements `atomic_add_flow(...)`, the entire graph is written
in one backend operation.

If it does not, AsyncMQ falls back to a safe sequential path:

- root jobs are enqueued first
- child dependencies are registered
- dependent jobs are then stored

Fallback mode is still correct for runtime execution, but it is not fully
atomic. If a failure happens halfway through flow creation, you may have a
partially created graph.

Production advice:

- keep flow creation idempotent
- use stable `job_id` values when repeat submissions are possible
- prefer Redis or Postgres if atomic graph creation matters operationally

## Runtime Dependency Resolution

At execution time, workers enforce dependencies in a backend-neutral way.

If a dequeued job still has unresolved parents:

- it is not executed
- it is moved back to `delayed` briefly
- it remains visible through `waiting-children` inspection APIs

When a parent completes, the backend removes that parent id from each affected
child. Once the last unresolved parent is removed, the child becomes runnable.

This means flow correctness does not depend solely on producer-side setup.
Workers still protect execution ordering.

## Failure Semantics

AsyncMQ flow behavior is intentionally simple and explicit:

- child jobs run only after all parents complete
- failed parents do not automatically trigger descendant failure
- descendants remain blocked until the parent is retried, repaired, or removed

This is an important operational difference from BullMQ's broader flow
ecosystem. AsyncMQ focuses on dependency gating rather than a large family of
parent-state policy options.

If you want cascade failure semantics, model them in one of these ways:

- a supervisor task that inspects parent state and removes blocked descendants
- explicit operator workflows using queue inspection and retry/remove actions
- application-level compensation logic

## Inspecting Flow State

The queue inspection APIs are the main way to inspect flow progress:

```python
waiting_children = await queue.get_waiting_children()
children_page = await queue.get_jobs(["waiting-children"], start=0, end=49)
parent_state = await queue.get_job_state(parent_id)
parent_job = await queue.get_job(parent_id)
```

Useful patterns:

- look at `waiting-children` to find blocked descendants
- inspect parent `failed` or `completed` state directly
- use `get_job_counts(...)` to monitor how much work is blocked on dependencies

## Backend Notes

All built-in backends support dependency-aware execution, but they do not all
offer the same write-time guarantees.

- Redis and Postgres are the strongest choices when you want more robust flow
  creation semantics and broader coordination guarantees
- MongoDB and in-memory keep the behavior portable but do not pretend to offer
  cross-process atomicity they do not have
- RabbitMQ flow behavior depends on its metadata store for dependency state

## BullMQ Mapping

| BullMQ | AsyncMQ |
| --- | --- |
| `FlowProducer` | `FlowProducer` |
| parent/child dependencies | `Job(..., depends_on=[...])` |
| waiting children inspection | `queue.get_waiting_children()` |
| flow creation | `await flow_producer.add_flow(queue, jobs)` |

AsyncMQ preserves the practical DAG creation model while keeping storage and
dependency bookkeeping portable across backends.

## Common Mistakes

- Using flows for loosely related jobs that could just run independently.
- Assuming flow creation is atomic on every backend.
- Assuming children will fail automatically when a parent fails.
- Forgetting to inspect `waiting-children` during incidents.

## Related

- [Jobs](jobs.md)
- [Queues](queues.md)
- [Workers](workers.md)
- [Advanced Patterns](../learn/advanced-patterns.md)
