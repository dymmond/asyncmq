# Custom JSON Serialization

AsyncMQ uses `settings.json_dumps` and `settings.json_loads` through `JSONSerializer`.

This lets you support payload types like `UUID`, `datetime`, and `Decimal`.

## Example

```python
import json
from datetime import datetime
from decimal import Decimal
from functools import partial
from uuid import UUID

from asyncmq.conf.global_settings import Settings


def encode(obj):
    if isinstance(obj, UUID):
        return {"__type__": "uuid", "value": str(obj)}
    if isinstance(obj, datetime):
        return {"__type__": "datetime", "value": obj.isoformat()}
    if isinstance(obj, Decimal):
        return {"__type__": "decimal", "value": str(obj)}
    raise TypeError(f"Unsupported: {type(obj)!r}")


def decode(value):
    if isinstance(value, dict) and value.get("__type__") == "uuid":
        from uuid import UUID

        return UUID(value["value"])
    if isinstance(value, dict) and value.get("__type__") == "datetime":
        return datetime.fromisoformat(value["value"])
    if isinstance(value, dict) and value.get("__type__") == "decimal":
        return Decimal(value["value"])
    return value


class AppSettings(Settings):
    json_dumps = partial(json.dumps, default=encode)
    json_loads = partial(json.loads, object_hook=decode)
```

## Guidance

- Keep encoder/decoder deterministic and backward-compatible.
- Prefer explicit typed envelopes (`{"__type__": ...}`) over implicit field-name heuristics.
- Add tests for round-trip serialization of task payloads and job metadata.
