# Jobs

`asyncmq.jobs.Job` is the serializable runtime object passed through backends and workers.

## Constructor Fields

```python
Job(
    task_id: str,
    args: list[Any],
    kwargs: dict[str, Any],
    retries: int = 0,
    max_retries: int = 3,
    backoff: float | int | Callable | None = 1.5,
    ttl: int | None = None,
    job_id: str | None = None,
    created_at: float | None = None,
    priority: int = 5,
    repeat_every: float | int | None = None,
    depends_on: list[str] | None = None,
)
```

## Serialization Contract

- `to_dict()` stores task id under key `task`.
- `from_dict()` expects keys like `id`, `task`, `args`, `kwargs`, `status`, `delay_until`.

This contract is important when writing custom backends or external tooling.

## State and Timing Helpers

- `is_expired()`: checks `ttl` against `created_at`.
- `next_retry_delay()`: computes delay from `backoff`.

`backoff` supports:

- numeric exponential base (`backoff ** retries`)
- callable with retry count
- callable without args
- `None` for immediate retry

## Runtime State

Default `job.status` is `waiting`. Workers and backends move jobs through lifecycle states (`waiting`, `active`, `completed`, `failed`, `delayed`, `expired`).

## Practical Guidance

- Keep `args`/`kwargs` JSON-serializable.
- Make task handlers idempotent.
- Use `ttl` for staleness-sensitive work.
- Use bounded retries and explicit backoff to protect dependencies.
