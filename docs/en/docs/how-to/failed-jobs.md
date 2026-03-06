# Handle Failed Jobs

## Understand Failure Paths

In worker runtime:

- retries available -> job becomes `delayed` (backoff) and is re-scheduled
- retries exhausted -> job becomes `failed` and is moved to DLQ

## Inspect Failures

```bash
asyncmq job list --queue default --state failed
asyncmq queue info default
```

Dashboard also exposes DLQ actions under `/queues/<name>/dlq`.

## Retry Failed Jobs

From CLI:

```bash
asyncmq job retry <job_id> --queue default
```

From dashboard:

- open queue DLQ page
- select job(s)
- run retry action

## Remove Poison Jobs

```bash
asyncmq job remove <job_id> --queue default
```

Use removal only when retry is known to be unsafe or irrelevant.

## Prevent Repeat Incidents

- make handlers idempotent
- cap retries and use backoff
- keep payloads small and deterministic
- add handler-level input validation
