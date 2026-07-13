# Deployment

This guide covers practical production deployment patterns for AsyncMQ worker,
producer, and dashboard processes.

## Process Model

Run these as separate process types whenever traffic is meaningful:

- producer applications that enqueue jobs
- worker processes or containers per queue family
- optional standalone stalled-recovery process
- dashboard/admin process

Avoid combining high-volume workers with user-facing HTTP traffic unless the
deployment is intentionally small and the failure blast radius is acceptable.

## Configuration

Set `ASYNCMQ_SETTINGS_MODULE` to a settings class that configures the backend
and runtime limits:

```bash
export ASYNCMQ_SETTINGS_MODULE=myapp.settings.AppSettings
```

Keep secrets, backend URLs, JWT/session keys, and dashboard credentials in the
platform secret manager rather than in image layers.

Recommended production settings to review:

- `backend`
- `worker_concurrency`
- `scan_interval`
- `enable_stalled_check`
- `stalled_threshold`
- `stalled_check_interval`
- `max_job_payload_bytes`
- dashboard authentication and CORS options

## Docker

A minimal worker image usually looks like this:

```dockerfile
FROM python:3.13-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY asyncmq ./asyncmq
COPY myapp ./myapp

RUN pip install --no-cache-dir .[all]

ENV ASYNCMQ_SETTINGS_MODULE=myapp.settings.AppSettings
CMD ["asyncmq", "worker", "start", "emails", "--concurrency", "8"]
```

Use one image with different commands for producer, worker, and dashboard
processes. Keep queue names and concurrency in deployment configuration so
operators can scale queues independently.

## Kubernetes

Worker deployments should set a termination grace period long enough for normal
job durations:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: asyncmq-emails-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: asyncmq-emails-worker
  template:
    metadata:
      labels:
        app: asyncmq-emails-worker
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        - name: worker
          image: registry.example.com/myapp:latest
          command: ["asyncmq", "worker", "start", "emails", "--concurrency", "8"]
          env:
            - name: ASYNCMQ_SETTINGS_MODULE
              value: myapp.settings.AppSettings
```

For dashboard deployments, wire probes to the built-in endpoints:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
readinessProbe:
  httpGet:
    path: /ready
    port: 8000
```

Scrape `/metrics/prometheus` when the dashboard is deployed in the same trust
boundary as your monitoring system.

## Rolling Restarts

For worker rollouts:

1. deploy replacement workers gradually
2. drain old workers when your process wrapper can signal `Worker.drain()` or
   `run_worker(..., drain_event=...)`; `asyncmq worker start ...` requests this
   drain path on SIGINT and SIGTERM
3. let in-flight jobs finish within `terminationGracePeriodSeconds`
4. rely on stalled recovery and idempotent handlers for workers terminated after
   the grace window
5. verify worker heartbeat freshness and queue depth after rollout

Queue-level `pause` is stronger than worker draining on backends with shared
pause state. Use pause only when you need all workers that observe that backend
state to stop claiming work.

## Backend Readiness

Production deployments should fail fast if the backend is unreachable at
startup. For SQL/document stores, initialize schema or indexes before sending
traffic. For RabbitMQ, ensure queue declaration arguments match the backend
configuration because RabbitMQ queue arguments are immutable after creation.

## Recovery Expectations

After process crash or node loss:

- Redis/PostgreSQL/MongoDB/RabbitMQ backends can release stale active jobs when
  stalled recovery is enabled and enough active-job metadata is persisted.
- RabbitMQ redelivers unacknowledged broker messages after connection loss.
- InMemory state is lost with the process and is not suitable for production.

See [Delivery Semantics](../reference/delivery-semantics.md) for the exact
runtime contract.
