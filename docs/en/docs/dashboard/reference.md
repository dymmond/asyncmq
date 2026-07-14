# Dashboard API and Route Reference

This page documents the dashboard routes and request parameters implemented in `asyncmq.contrib.dashboard`.

## Route Map

Routes are shown relative to the configured dashboard mount or URL prefix. For
example, with the default `/asyncmq` prefix, `/health` is served at
`/asyncmq/health`.

| Route | Methods | Purpose |
| --- | --- | --- |
| `/` | `GET` | Overview page (totals + live charts/tables) |
| `/health` | `GET` | Liveness probe for the dashboard process |
| `/ready` | `GET` | Readiness probe that runs a lightweight backend reachability check |
| `/queues` | `GET` | Queue list with state counts |
| `/queues/{name}` | `GET`, `POST` | Queue detail + pause/resume actions |
| `/queues/{name}/jobs` | `GET`, `POST` | Job list with filtering/search and bulk actions |
| `/queues/{name}/jobs/{job_id}/{action}` | `POST` | Single job action API (`retry`, `remove`, `cancel`) |
| `/queues/{name}/dlq` | `GET`, `POST` | DLQ listing and retry/remove actions |
| `/queues/{name}/repeatables` | `GET`, `POST` | Repeatable definitions and actions |
| `/queues/{name}/repeatables/new` | `GET`, `POST` | Create a repeatable definition |
| `/workers` | `GET` | Active worker list |
| `/metrics` | `GET` | Metrics view and charts |
| `/metrics/history` | `GET` | Metrics history JSON API |
| `/metrics/prometheus` | `GET` | Prometheus text exposition for dashboard-visible queue and worker gauges |
| `/audit` | `GET` | Queue/job action audit trail view |
| `/events` | `GET` | Server-Sent Events stream |
| `/events/history` | `GET` | Bounded local runtime event history view |

## Job List Query Parameters

`GET /queues/{name}/jobs`

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| `state` | string | `waiting` | One of `waiting`, `active`, `completed`, `failed`, `delayed`, `repeatable` |
| `page` | integer | `1` | 1-based page index |
| `size` | integer | `20` | Items per page |
| `q` | string | empty | Full-text payload search (`json.dumps` style match) |
| `task` | string | empty | Matches `task_id` / `task` / `name` fields |
| `job_id` | string | empty | Partial match against job id |
| `sort` | string | `newest` | `newest` or `oldest`, based on run/creation timestamps |

### Example URLs

```text
/queues/emails/jobs?state=failed&task=send-email&sort=newest
/queues/emails/jobs?state=waiting&q=customer-123&job_id=7f3a&page=2&size=50
```

## Audit Trail Filters

`GET /audit`

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| `action` | string | empty | Exact action name filter (for example `job.retry`) |
| `status` | string | empty | `success` or `failed` |
| `queue` | string | empty | Queue name filter |
| `q` | string | empty | Full-text search across action/source/queue/job/actor/error/details |
| `limit` | integer | `200` | Clamped to `1..500` |

### Audit Event Shape

```json
{
  "timestamp": 1741265786.204,
  "action": "job.retry",
  "source": "jobs.single",
  "status": "success",
  "queue": "emails",
  "job_id": "f1",
  "actor": "ops-user",
  "details": {},
  "error": null
}
```

## Metrics History API

`GET /metrics/history?limit=120`

Response shape:

```json
{
  "history": [
    {
      "timestamp": 1741265786.204,
      "time": "2026-03-06T15:49:46.204000+00:00",
      "throughput": 42,
      "avg_duration": null,
      "retries": 3,
      "failures": 3,
      "waiting": 5,
      "active": 2,
      "delayed": 1,
      "completed": 42,
      "failed": 3,
      "total_queues": 4,
      "total_workers": 8
    }
  ]
}
```

## Prometheus Metrics

`GET /metrics/prometheus` returns `text/plain; version=0.0.4` exposition
derived from dashboard-visible backend inspection.

Current gauges:

- `asyncmq_dashboard_ready`
- `asyncmq_queue_total`
- `asyncmq_worker_total`
- `asyncmq_queue_jobs{queue="...",state="..."}`

If backend inspection fails, the endpoint returns `503` and emits
`asyncmq_dashboard_ready 0`.

## Health and Readiness

`GET /health` returns process liveness without touching the queue backend:

```json
{"status": "ok", "service": "asyncmq-dashboard"}
```

`GET /ready` runs the backend's lightweight reachability check without
enumerating queues or workers. It returns `200` with `status: "ok"` when the
backend is reachable and `503` with `status: "error"` when the check raises.

## SSE Events

`GET /events` emits these event names:

- `overview`
- `jobdist`
- `metrics`
- `queues`
- `workers`
- `latest_jobs`
- `latest_queues`

### SSE Consumer Example

```javascript
const source = new EventSource('/asyncmq/events');
source.addEventListener('metrics', (event) => {
  const payload = JSON.parse(event.data);
  console.log('throughput', payload.throughput);
});
```

For operational runbooks, see [Dashboard Operations Playbook](operations.md).
