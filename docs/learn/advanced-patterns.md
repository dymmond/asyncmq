# Advanced Patterns

Welcome to the secret sauce of AsyncMQ where we go beyond the basics into **Custom Backends**,
**DAG Orchestration**, and **Kubernetes Scaling**.

If you thought AsyncMQ was cool before, strap in: it’s about to get wild.

---

## 1. Custom Backends

Sometimes Redis, Postgres, or MongoDB just don’t cut it. Maybe you need to push tasks into an SQS queue, or write to
Kafka, or integrate with a proprietary message bus. Here’s how:

### 1.1. Understand `BaseBackend`

All AsyncMQ backends implement the **`BaseBackend` protocol** (in `asyncmq.backends.base`), which defines methods such as:

```python
async def enqueue(self, queue: str, job_dict: dict) -> None: ...
async def enqueue_delayed(self, queue: str, job_dict: dict, run_at: float) -> None: ...
async def dequeue(self, queue: str, timeout: int) -> dict | None: ...
async def add_dependencies(self, queue: str, job_dict: dict) -> None: ...
async def get_jobs(self, queue: str, state: State) -> list[dict]: ...
# plus DLQ, ack, cancel, pause/resume methods and a lot more
```

### 1.2. Implementing Your Backend

```python
{!> ../docs_src/tutorial/custom_backend.py !}
```

You must implement the remaining methods: `enqueue_delayed` (using SQS DelaySeconds), add_dependencies
(store in DynamoDB?), move_to_dlq, get_jobs (DynamoDB scan?), etc.

Everything is in the `BaseBackend` and `BaseStore` to follow.

**Why bother?**

* Leverage existing infrastructure (SQS, Kafka...)
* Meet compliance requirements
* Offload scaling concerns to a managed service

!!! Tip
    If your queuing system doesn’t support delayed messages natively, emulate delays by storing messages in a secondary
    store (Redis sorted set, DynamoDB TTL) and poll for ready items in a custom scanner.

---

## 2. DAG Orchestration with `FlowProducer`

Chaining tasks into multi-step pipelines, no more ad-hoc glue scripts.

### 2.1. Building a DAG

```python
{!> ../docs_src/tutorial/flow_producer.py !}
```

### 2.2. Conditional and Parallel Steps

Use `depends_on` lists to fan-out parallel tasks or gate steps based on multiple dependencies.

```python
# Two parallel transforms after ingestion
job_transform_a = Job(..., depends_on=[job_ingest.id])
job_transform_b = Job(..., depends_on=[job_ingest.id])
# Final step waits for both
job_aggregate = Job(
    ..., depends_on=[job_transform_a.id, job_transform_b.id]
)
await flow.add_flow("parallel_pipeline", [job_ingest, job_transform_a, job_transform_b, job_aggregate])
```

### 2.3. Repeatable and Cron Jobs

```python
# Run cleanup every day at midnight
from croniter import croniter
job_cleanup = Job(
    task_id="app.cleanup_temp",
    args=[],
    cron="0 0 * * *"
)
await flow.add_flow("maintenance", [job_cleanup])
```

!!! Note
    **Inside scoop:** `FlowProducer` uses `croniter` to calculate next run times and schedules them via the delayed scanner.

    Bonus: custom backoff and jitter can be injected if you need resilience.

---

## 3. Kubernetes Scaling

Your AsyncMQ workers can scale elegantly in Kubernetes. Let’s break down a typical setup:

### 3.1. Redis Deployment

Use the **Bitnami Redis** Helm chart or an equivalent managed service:

```yaml
# redis-deployment.yaml\ napiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 500m
            memory: 256Mi
```

### 3.2. Worker Deployment

```yaml
# asyncmq-worker.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: asyncmq-worker
spec:
  replicas: 3                      # start with 3 workers
  selector:
    matchLabels:
      app: asyncmq-worker
  template:
    metadata:
      labels:
        app: asyncmq-worker
    spec:
      containers:
      - name: worker
        image: myrepo/asyncmq-worker:latest
        env:
        - name: ASYNCMQ_SETTINGS_MODULE
          value: "myapp.settings.Settings"
        args: ["worker", "start", "default"]
        resources:
          requests:
            cpu: 200m
            memory: 256Mi
          limits:
            cpu: 1
            memory: 512Mi
        readinessProbe:
          exec:
            command: ["/bin/sh", "-c", "asyncmq job list --queue default --state waiting"]
          initialDelaySeconds: 10
          periodSeconds: 15
```

### 3.3. Autoscaling with KEDA

Use KEDA to autoscale based on Redis list length:

```yaml
# keda-scaledobject.yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: asyncmq-worker-scaledobject
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: asyncmq-worker
  triggers:
  - type: redis
    metadata:
      address: "redis://redis:6379"
      listName: "default:jobs"
      listLength: "10"    # scale up if >10 jobs waiting
```

### 3.4. Tips & Gotchas

* **Resource Tuning**: Keep your CPU and memory requests small for event loop tasks; avoid OOM kills.
* **Probe wisely**: Readiness should check that the worker process is up, not that jobs exist—otherwise scaling may misinterpret an empty queue as unhealthy.
* **Secrets**: Store Redis credentials in Kubernetes Secrets; mount as env vars or files.
* **Logging & Monitoring**: Combine `event_emitter` hooks with Prometheus or ELK stack for real-time insights.

!!! Joke
    **Final joke**: If your cluster autoscaler has better reflexes than you do on Monday mornings, you’re doing it right.

---

That’s a wrap on Advanced Patterns! Up next: **Performance Tuning & Benchmarking** We’ll squeeze every last millisecond
out of AsyncMQ.
