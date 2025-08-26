"""
Example of configuring custom JSON encoding/decoding in AsyncMQ settings.

This example shows how to handle common data types like UUID, datetime, and Decimal
that are not natively JSON-serializable.
"""

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
    """Custom settings with JSON encoding/decoding for common data types."""
    json_dumps = partial(json.dumps, default=custom_json_encoder)
    json_loads = partial(json.loads, object_hook=custom_json_decoder)


# Example usage
if __name__ == "__main__":
    settings = Settings()

    # Test data with custom types
    test_data = {
        "user_id": uuid.uuid4(),
        "session_id": uuid.uuid4(),
        "created_at": datetime.now(),
        "amount": Decimal("123.45"),
        "task": "process_user_data"
    }

    print("Original data:")
    print(f"user_id: {test_data['user_id']} (type: {type(test_data['user_id'])})")
    print(f"created_at: {test_data['created_at']} (type: {type(test_data['created_at'])})")
    print(f"amount: {test_data['amount']} (type: {type(test_data['amount'])})")

    # Serialize to JSON
    json_str = settings.json_dumps(test_data)
    print(f"\nSerialized JSON:\n{json_str}")

    # Deserialize from JSON
    restored_data = settings.json_loads(json_str)
    print(f"\nRestored data:")
    print(f"user_id: {restored_data['user_id']} (type: {type(restored_data['user_id'])})")
    print(f"created_at: {restored_data['created_at']} (type: {type(restored_data['created_at'])})")
    print(f"amount: {restored_data['amount']} (type: {type(restored_data['amount'])})")

    # Verify round-trip
    assert restored_data["user_id"] == test_data["user_id"]
    assert restored_data["session_id"] == test_data["session_id"]
    assert restored_data["created_at"] == test_data["created_at"]
    print("\n✅ Round-trip serialization successful!")
