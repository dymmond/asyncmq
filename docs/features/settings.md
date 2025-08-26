# Settings

The **`Settings`** dataclass is the beating heart of your AsyncMQ installation.
It centralizes every configuration option, think of it as the master control panel for AsyncMQ’s behavior.
Customize this once, and your entire queueing system behaves predictably across environments.


**Why a Settings Class?**

- **Single Source of Truth**: Keep all defaults and overrides in one place.
- **Type Safety & Discoverability**: IDEs gently guide you through each field.
- **Environment Drift Protection**: Export a single `ASYNCMQ_SETTINGS_MODULE` and never worry about mismatched values.

---

## 1. What Is `Settings`?

Located in `asyncmq.conf.global_settings`, the `Settings` dataclass defines:

* **Global flags** (`debug`, `logging_level`).
* **Backend configuration** (`backend`, Postgres/Mongo URLs, table names).
* **Worker parameters** (`worker_concurrency`).
* **Schedulers** (`scan_interval`, `enable_stalled_check`, `stalled_*`).
* **Sandbox** controls (`sandbox_enabled`, `sandbox_timeout`, `sandbox_ctx`).

Every AsyncMQ component reads values from a single `Settings()` instance, ensuring consistent behavior.

You can import the `Settings` via:

```python
from asyncmq import Settings
```

---

## 2. Why It Exists

* **Modularity**: Swap Redis for Postgres by changing `backend`, no code changes.
* **Scalability**: Tune concurrency, rate limits, and scan intervals for production loads.
* **Safety**: Enable stalled-job detectors and sandboxing to guard against rogue tasks.
* **Observability**: Control logging levels and hook into audit trails with minimal code.

!!! Tip
    Treat `Settings` like a vehicle’s dashboard: know where each dial lives and what happens when you turn it! 😉

---

## 3. How to Use It

The settings is a pure Python `dataclass` and it should be treated as such.

1. **Subclass** the global defaults:

   ```python
   from asyncmq.conf.global_settings import Settings as BaseSettings
   from asyncmq.backends.redis import RedisBackend
   from asyncmq.backends.base import BaseBackend

   class Settings(BaseSettings):
       debug: bool = True
       logging_level: str = "DEBUG"
       backend: BaseBackend = RedisBackend(redis_url_or_client="rediss://redis:6379/0")
       worker_concurrency: int = 5
       scan_interval: float = 0.5
       enable_stalled_check: bool = True
       stalled_threshold: float = 60.0
   ```
2. **Point AsyncMQ** at your class using the environment variable:

   ```bash
   export ASYNCMQ_SETTINGS_MODULE=myapp.settings.Settings
   ```
3. **Start your app** or **run workers**, they will pick up your custom settings automatically.


**Why `ASYNCMQ_SETTINGS_MODULE`?**

- Allows you to keep sensitive or environment-specific settings out of code.
- Enables different settings in dev/staging/production without code changes.
- Avoids hardcoding credentials, connection strings, or toggles.


**Full Resolution Logic**:

1. At import, AsyncMQ reads `ASYNCMQ_SETTINGS_MODULE` env var.
2. Dynamically imports that class and instantiates it.
3. Falls back to `asyncmq.conf.global_settings.Settings` if var is unset.
4. All components reference this singular `settings` object.

---

## 4. Custom Settings: Deep Dive

Beyond basic overrides, you can leverage `Settings.dict()` and `Settings.tuple()` to integrate with orchestration tools
or dotenv files:

```python
import os
from asyncmq.conf.global_settings import Settings

# Dump current settings to env-like variables
for k, v in Settings().dict(exclude_none=True, upper=True).items():
    os.environ[k] = str(v)
```

Or generate a config file:

```python
import yaml
from asyncmq.conf.global_settings import Settings

with open('asyncmq_config.yml', 'w') as f:
    yaml.safe_dump(Settings().dict(), f)
```

!!! Tip
    **Tip:** Use `upper=True` to conform to conventional ENV_VAR naming.

---

## 5. Detailed Settings Reference

Below is a distilled breakdown of each setting, why it matters, and tips for tuning:

| Name                                   | Type            | Default                    | Purpose & Tips                                                                             |
|----------------------------------------|-----------------|----------------------------| ------------------------------------------------------------------------------------------ |
| **debug**                              | `bool`          | `False`                    | Turn on detailed stack traces & verbose logs when diagnosing issues.                       |
| **logging_level**                      | `str`           | `"INFO"`                   | Control verbosity. Use `DEBUG` for development, `ERROR` for stable production.             |
| **backend**                            | `BaseBackend`   | `RedisBackend()`           | Swap storage/backing queue. Use Postgres or Mongo backends for ACID or schema flexibility. |
| **version**                            | `str`           | AsyncMQ version            | Handy for logging and health endpoints.                                                    |
| **is_logging_setup**                   | `bool`          | `False`                    | Internal flag—rarely override manually.                                                    |
| **jobs_table_schema**                  | `str`           | `"asyncmq"`                | Postgres schema. Namespace your tables to avoid collisions.                                |
| **postgres_jobs_table_name**           | `str`           | `"asyncmq_jobs"`           | Table name for jobs in Postgres. Change for multi-tenant setups.                           |
| **postgres_repeatables_table_name**    | `str`           | `"asyncmq_repeatables"`    | Stores repeatable job definitions in Postgres.                                             |
| **postgres_cancelled_jobs_table_name** | `str`           | `"asyncmq_cancelled_jobs"` | Tracks cancelled jobs in Postgres.                                                         |
| **asyncmq_postgres_backend_url**       | `str            | None`                      | `None`                     | DSN for Postgres. Required if using Postgres backend.                                      |
| **asyncmq_postgres_pool_options**      | `dict[str, Any] | None`                      | `None`                     | Options for `asyncpg.create_pool`. Tunes connection pooling.                               |
| **asyncmq_mongodb_backend_url**        | `str            | None`                      | `None`                     | URI for MongoDB. Use `mongodb+srv` URIs with credentials.                                  |
| **asyncmq_mongodb_database_name**      | `str            | None`                      | `"asyncmq"`                | Target DB in MongoDB.                                                                      |
| **enable_stalled_check**               | `bool`          | `False`                    | Detect jobs stuck > `stalled_threshold`. Useful for long tasks.                            |
| **stalled_check_interval**             | `float`         | `60.0`                     | Scan frequency for stalled jobs. Lower for faster detection, higher for less DB load.      |
| **stalled_threshold**                  | `float`         | `30.0`                     | Execution time considered stalled. Tune per workload.                                      |
| **sandbox_enabled**                    | `bool`          | `False`                    | Run tasks in isolated processes. Safer but slower.                                         |
| **sandbox_default_timeout**            | `float`         | `30.0`                     | Max seconds for sandboxed job. Prevent runaway tasks.                                      |
| **sandbox_ctx**                        | `str            | None`                      | `"fork"`                   | Multiprocessing context for sandbox. OS-dependent.                                         |
| **worker_concurrency**                 | `int`           | `1`                        | Default concurrency for CLI workers. Increase for I/O-bound tasks.                         |
| **tasks**                               | `list[str]`     | `[]`                       | Python modules or packages to auto-discover your `@task(…)` definitions at worker startup. Defaults to `[]`.                         |
| **scan_interval**                      | `float`         | `1.0`                      | Global frequency to poll delayed & repeatable jobs. Override per-queue if needed.          |
| **json_dumps**                         | `Callable`      | `json.dumps`               | Custom JSON serialization function. Configure to handle custom data types like UUID, datetime, Decimal. |
| **json_loads**                         | `Callable`      | `json.loads`               | Custom JSON deserialization function. Configure to restore custom data types from JSON.     |
| **json_serializer**                    | `JSONSerializer` | Auto-created              | Centralized JSON serializer that handles both regular and partial functions correctly.      |

!!! Note
    Fields like `asyncmq_postgres_pool_options` and table names exist for advanced customizations.
    See code docstrings for exhaustive details.

---

## 6. Pros & Advantages

* **Rapid Iteration**: Changing settings doesn’t require code redeploy, just point at a new `Settings` subclass.
* **Environment Parity**: Keep staging & prod configs in code, reducing *“it works only locally”* surprises.
* **Self-Documentation**: Dataclass defaults and docstrings serve as living API docs.
* **Extensibility**: Add new fields easily without rewriting initialization logic.

---

## 7. Custom JSON Encoding/Decoding

AsyncMQ allows you to customize how job data and payloads are serialized to and from JSON. This is particularly useful when working with custom data types that aren't natively JSON-serializable, such as `UUID`, `datetime`, `Decimal`, or custom classes.

### Why Custom JSON Encoding?

By default, AsyncMQ uses Python's standard `json.dumps` and `json.loads` functions. However, these don't handle many common data types:

```python
import json
import uuid

# This will fail with TypeError
job_data = {"user_id": uuid.uuid4(), "task": "process_user"}
json.dumps(job_data)  # TypeError: Object of type UUID is not JSON serializable
```

### Configuring Custom JSON Functions

You can configure custom JSON serialization functions in your settings:

```python
import json
import uuid
from datetime import datetime
from decimal import Decimal
from functools import partial
from asyncmq.conf.global_settings import Settings as BaseSettings

def custom_json_encoder(obj):
    """Custom encoder that handles UUID, datetime, and Decimal objects."""
    if isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def custom_json_decoder(dct):
    """Custom decoder that converts strings back to appropriate types."""
    for key, value in dct.items():
        if isinstance(value, str):
            # Try to parse UUIDs (for keys ending with '_id')
            if key.endswith('_id'):
                try:
                    dct[key] = uuid.UUID(value)
                except ValueError:
                    pass  # Not a valid UUID, keep as string
            # Try to parse ISO datetime strings
            elif key.endswith('_at') or key.endswith('_time'):
                try:
                    dct[key] = datetime.fromisoformat(value)
                except ValueError:
                    pass  # Not a valid datetime, keep as string
    return dct

class Settings(BaseSettings):
    json_dumps = partial(json.dumps, default=custom_json_encoder)
    json_loads = partial(json.loads, object_hook=custom_json_decoder)
```

### Real-World Example: E-commerce Order Processing

Here's a practical example for an e-commerce system:

```python
import json
import uuid
from datetime import datetime
from decimal import Decimal
from functools import partial
from asyncmq.conf.global_settings import Settings as BaseSettings

def ecommerce_json_encoder(obj):
    """Encoder for e-commerce data types."""
    if isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return str(obj)  # Keep precision for money
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def ecommerce_json_decoder(dct):
    """Decoder for e-commerce data types."""
    for key, value in dct.items():
        if isinstance(value, str):
            # Handle UUIDs
            if key in ['order_id', 'user_id', 'product_id', 'session_id']:
                try:
                    dct[key] = uuid.UUID(value)
                except ValueError:
                    pass
            # Handle timestamps
            elif key in ['created_at', 'updated_at', 'shipped_at']:
                try:
                    dct[key] = datetime.fromisoformat(value)
                except ValueError:
                    pass
            # Handle money amounts
            elif key in ['price', 'total', 'tax', 'shipping_cost']:
                try:
                    dct[key] = Decimal(value)
                except (ValueError, TypeError):
                    pass
    return dct

class Settings(BaseSettings):
    json_dumps = partial(json.dumps, default=ecommerce_json_encoder)
    json_loads = partial(json.loads, object_hook=ecommerce_json_decoder)
```

### Using Custom JSON with Tasks

Once configured, your custom JSON functions work automatically with all AsyncMQ operations:

```python
import uuid
from datetime import datetime
from decimal import Decimal
from asyncmq import task, Queue

@task
async def process_order(order_data):
    # order_data will have proper types restored
    print(f"Processing order {order_data['order_id']}")  # UUID object
    print(f"Total: ${order_data['total']}")  # Decimal object
    print(f"Created: {order_data['created_at']}")  # datetime object

# Enqueue with custom data types
queue = Queue("orders")
await queue.enqueue(process_order, {
    "order_id": uuid.uuid4(),
    "user_id": uuid.uuid4(),
    "total": Decimal("99.99"),
    "created_at": datetime.now(),
    "items": [
        {"product_id": uuid.uuid4(), "price": Decimal("49.99")},
        {"product_id": uuid.uuid4(), "price": Decimal("50.00")},
    ]
})
```

### Best Practices

1. **Handle Errors Gracefully**: Always include try/catch blocks in your decoder to handle malformed data.

2. **Be Specific**: Use field names or patterns to identify which values to convert (e.g., `*_id` for UUIDs).

3. **Preserve Precision**: For financial data, use `str()` for Decimal encoding to maintain precision.

4. **Test Thoroughly**: Ensure your encoder/decoder pair can handle round-trip serialization:

```python
def test_round_trip():
    original = {"user_id": uuid.uuid4(), "amount": Decimal("123.45")}
    json_str = settings.json_dumps(original)
    restored = settings.json_loads(json_str)
    assert restored["user_id"] == original["user_id"]
    assert restored["amount"] == original["amount"]
```

5. **Document Your Schema**: Clearly document which fields use which data types for your team.

### Performance Considerations

- Custom JSON functions add overhead to serialization/deserialization
- Consider using simpler encoders for high-throughput scenarios
- Profile your custom functions if performance is critical
- Cache compiled regex patterns if using pattern matching in decoders

---

Now you’re a Settings maestro, tune every aspect of AsyncMQ with confidence and style! 🎶
