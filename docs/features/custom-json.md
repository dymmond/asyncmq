# Custom JSON Encoding/Decoding

AsyncMQ provides powerful customization options for JSON serialization and deserialization, allowing you to work seamlessly with custom data types that aren't natively JSON-serializable.

## Overview

By default, AsyncMQ uses Python's standard `json.dumps` and `json.loads` functions for serializing job data and payloads. However, many common Python data types like `UUID`, `datetime`, `Decimal`, and custom classes cannot be directly serialized to JSON.

Custom JSON encoding solves this problem by allowing you to define how these types should be converted to and from JSON format.

## Quick Start

Here's a simple example of configuring custom JSON encoding for UUID objects:

```python
import json
import uuid
from functools import partial
from asyncmq.conf.global_settings import Settings as BaseSettings

def uuid_encoder(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def uuid_decoder(dct):
    for key, value in dct.items():
        if key.endswith('_id') and isinstance(value, str):
            try:
                dct[key] = uuid.UUID(value)
            except ValueError:
                pass
    return dct

class Settings(BaseSettings):
    json_dumps = partial(json.dumps, default=uuid_encoder)
    json_loads = partial(json.loads, object_hook=uuid_decoder)
```

## Common Use Cases

### 1. UUID Handling

UUIDs are commonly used as identifiers in modern applications:

```python
--8<-- "docs_src/settings/custom_json.py:33:50"
```

### 2. Financial Applications

For applications handling money, precise decimal arithmetic is crucial:

```python
def financial_encoder(obj):
    if isinstance(obj, Decimal):
        return str(obj)  # Preserve precision
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def financial_decoder(dct):
    for key, value in dct.items():
        if isinstance(value, str):
            if key in ['price', 'amount', 'total', 'balance']:
                try:
                    dct[key] = Decimal(value)
                except ValueError:
                    pass
    return dct
```

### 3. Datetime Serialization

Handle datetime objects with timezone information:

```python
from datetime import datetime

def datetime_encoder(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def datetime_decoder(dct):
    for key, value in dct.items():
        if isinstance(value, str) and key.endswith(('_at', '_time')):
            try:
                dct[key] = datetime.fromisoformat(value)
            except ValueError:
                pass
    return dct
```

## Complete Examples

### E-commerce Order Processing

--8<-- "docs_src/settings/ecommerce_json.py"

### Task Integration

--8<-- "docs_src/settings/task_with_custom_json.py"

## Best Practices

### 1. Error Handling

Always handle conversion errors gracefully:

```python
def safe_decoder(dct):
    for key, value in dct.items():
        if key.endswith('_id') and isinstance(value, str):
            try:
                dct[key] = uuid.UUID(value)
            except ValueError:
                # Log the error if needed, but don't fail
                pass
    return dct
```

### 2. Field Name Conventions

Use consistent naming patterns to identify data types:

- `*_id` for UUIDs
- `*_at`, `*_time` for timestamps
- `price`, `amount`, `total` for monetary values

### 3. Testing Round-Trip Serialization

Always test that your encoder/decoder pair works correctly:

```python
def test_round_trip_serialization():
    settings = Settings()

    original_data = {
        "user_id": uuid.uuid4(),
        "created_at": datetime.now(),
        "amount": Decimal("123.45")
    }

    # Serialize and deserialize
    json_str = settings.json_dumps(original_data)
    restored_data = settings.json_loads(json_str)

    # Verify data integrity
    assert restored_data["user_id"] == original_data["user_id"]
    assert restored_data["created_at"] == original_data["created_at"]
    assert restored_data["amount"] == original_data["amount"]
```

### 4. Performance Considerations

- Custom JSON functions add processing overhead
- Cache compiled regex patterns if using pattern matching
- Consider simpler encoders for high-throughput scenarios
- Profile your custom functions in production workloads

### 5. Documentation

Document your JSON schema for your team:

```python
"""
JSON Schema for Order Processing:

Fields with custom encoding:
- *_id: UUID objects (order_id, user_id, product_id, etc.)
- *_at, *_time: datetime objects (created_at, updated_at, etc.)
- price, total, tax, shipping_cost: Decimal objects for monetary values
"""
```

## Advanced Patterns

### Nested Object Handling

For complex nested structures:

```python
def recursive_decoder(dct):
    """Recursively decode nested dictionaries."""
    for key, value in dct.items():
        if isinstance(value, dict):
            dct[key] = recursive_decoder(value)
        elif isinstance(value, list):
            dct[key] = [recursive_decoder(item) if isinstance(item, dict) else item for item in value]
        elif isinstance(value, str):
            # Apply your type conversions here
            if key.endswith('_id'):
                try:
                    dct[key] = uuid.UUID(value)
                except ValueError:
                    pass
    return dct
```

### Custom Class Serialization

For your own classes:

```python
class Address:
    def __init__(self, street, city, zip_code):
        self.street = street
        self.city = city
        self.zip_code = zip_code

    def to_dict(self):
        return {
            "street": self.street,
            "city": self.city,
            "zip_code": self.zip_code,
            "__type__": "Address"
        }

    @classmethod
    def from_dict(cls, data):
        return cls(data["street"], data["city"], data["zip_code"])

def custom_class_encoder(obj):
    if isinstance(obj, Address):
        return obj.to_dict()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def custom_class_decoder(dct):
    if dct.get("__type__") == "Address":
        return Address.from_dict(dct)
    return dct
```

## Troubleshooting

### Common Issues

1. **TypeError during serialization**: Your encoder doesn't handle a specific type
   - Add handling for the missing type in your encoder function

2. **Data loss during round-trip**: Decoder isn't properly restoring types
   - Check your field name patterns and conversion logic

3. **Performance degradation**: Custom JSON functions are too slow
   - Profile your functions and optimize hot paths
   - Consider caching compiled patterns

### Debugging Tips

Enable debug logging to see serialization issues:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Your custom encoder with logging
def debug_encoder(obj):
    logging.debug(f"Encoding object: {obj} (type: {type(obj)})")
    # ... your encoding logic
```

## Migration Guide

### From Standard JSON

If you're migrating from standard JSON encoding:

1. **Identify custom types**: Find all non-JSON-serializable types in your codebase
2. **Create encoders**: Write encoder functions for each type
3. **Create decoders**: Write corresponding decoder functions
4. **Test thoroughly**: Ensure round-trip serialization works
5. **Deploy gradually**: Test in staging before production

### Backward Compatibility

When adding custom JSON encoding to an existing system:

```python
def backward_compatible_decoder(dct):
    """Decoder that handles both old and new data formats."""
    for key, value in dct.items():
        if isinstance(value, str) and key.endswith('_id'):
            # Only try to convert if it looks like a UUID
            if len(value) == 36 and value.count('-') == 4:
                try:
                    dct[key] = uuid.UUID(value)
                except ValueError:
                    pass  # Keep as string for backward compatibility
    return dct
```

This ensures that existing serialized data continues to work while new data benefits from proper type restoration.
