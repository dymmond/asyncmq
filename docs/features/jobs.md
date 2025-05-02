# Jobs & `Job` Abstraction

The `Job` class is the atomic unit in AsyncMQ—it encapsulates everything needed to execute, track, and manage a
single piece of work. This guide walks through its design, states, retry strategies, serialization, and how you can
extend or inspect jobs when building robust pipelines.

---

## 1. What Is a `Job`?

A `Job` represents a single invocation of your task:

* **Identity**: Unique `id` (UUID) and `task_id` (module.function name).
* **Payload**: Positional `args` and keyword `kwargs` for the handler.
* **Metadata**: `created_at`, `status`, `priority`, `depends_on`, `repeat_every`.
* **Lifecycle config**: `ttl`, `max_retries`, `backoff` (delay strategy).
* **Result & Errors**: Captures `result`, `last_error`, and `error_traceback` on failure.

!!! Analogy
    Think of a `Job` as a lab sample tube: it holds the specimen (your args), a barcode (id), and metadata about how
    to process it.

---

## 2. Job States & Transitions

All jobs move through these states:

```python
JOB_STATES = (
    State.WAITING,
    State.ACTIVE,
    State.COMPLETED,
    State.FAILED,
    State.DELAYED,
    State.EXPIRED,
)
```

* **WAITING**: Enqueued, awaiting a worker.
* **ACTIVE**: Being processed.
* **COMPLETED**: Finished successfully.
* **FAILED**: Retries exhausted, moved to DLQ.
* **DELAYED**: Scheduled for future retry or delay.
* **EXPIRED**: TTL exceeded before processing.

State changes occur in `handle_job()`, each transition emits events via `event_emitter`.

---

## 3. Construction & Serialization

### 3.1. Creating a Job

```python
from asyncmq.jobs import Job

job = Job(
    task_id="app.send_email",
    args=["user@example.com"],
    kwargs={"subject": "Hi"},
    max_retries=2,
    backoff=2.0,
    ttl=300,
    priority=3,
)
```

* **Default `id`**: Auto-generated UUID if not provided.
* **`created_at`**: Timestamp when instantiated.

### 3.2. `to_dict()` & `from_dict()`

Serialize for storage or transport:

```python
data = job.to_dict()
# persist `data` in Redis, Postgres, etc.
restored = Job.from_dict(data)
```

* **Persistence**: Backends round-trip jobs via dicts.
* **Extensibility**: Add new fields easily and handle in `to_dict`/`from_dict`.

---

## 4. Time-to-Live (TTL)

A TTL ensures stale jobs don’t linger:

```python
if job.is_expired():
    # move to DLQ, emit job:expired
```

```python
def is_expired(self) -> bool:
    return self.ttl is not None and (time.time() - self.created_at) > self.ttl
```

* **Use case**: Drop outdated tasks (e.g., stale notifications).
* **Pitfall**: Clock skew—ensure system times are in sync.

---

## 5. Retry & Backoff Strategies

### 5.1. Exponential Backoff

```python
def next_retry_delay(self) -> float:
    if isinstance(self.backoff, (int, float)):
        return float(self.backoff) ** self.retries
```

* **Example**: `backoff=2`, retries=3 ⇒ delay=8s.

### 5.2. Custom Callable Backoff

```python
# Linear backoff: 5s per retry
job.backoff = lambda n: 5 * n
```

* **Callable**: Accepts either retry count or no args.

### 5.3. No Backoff

* **`backoff=None`** ⇒ immediate retry.

Pitfall: rapid retries can overwhelm downstream systems—tune backoff accordingly.

---

## 6. Dependencies & Repeatables

* **`depends_on`**: List of job IDs; this job won’t run until dependencies complete.
* **`repeat_every`**: If set, indicates a repeatable job definition; the scheduler uses this to re-enqueue.

Use `FlowProducer` for DAGs or `Queue.add_repeatable()` for simple intervals.

---

## 7. Examples & Best Practices

### 7.1. Custom Job Subclassing

Though not common, you can subclass `Job` to add fields:

```python
class MyJob(Job):
    def __init__(self, user_id: str, **kwargs):
        super().__init__(**kwargs)
        self.user_id = user_id

    def to_dict(self):
        d = super().to_dict(); d['user_id'] = self.user_id; return d

    @staticmethod
    def from_dict(data):
        job = MyJob(user_id=data['user_id'], **data)
        return job
```

* **Caveat**: Backends must know to reconstruct your subclass.

### 7.2. Monitoring Job Metrics

Subscribe to events:

```python
from asyncmq.core.event import event_emitter

count = 0
@event_emitter.on('job:completed')
def on_complete(payload):
    count += 1
```

* **Track**: queue depth, latencies, retry rates.

---

## 8. Common Pitfalls & FAQs

* **Unpickleable Args**: Ensure `args` and `kwargs` are JSON/pickle-friendly (no open sockets).
* **Backoff Growth**: Exponential backoff can overflow—monitor or cap delay.
* **TTL vs. Retry**: A job may expire before retrying—order of checks matters.
* **Clock Skew**: In distributed setups, ensure all workers share synchronized clocks.

---

With the `Job` abstraction mastered, you control state transitions, retry behavior, and serialization, giving you full
visibility and confidence in your background tasks.
