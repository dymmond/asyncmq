import json
import uuid
from functools import partial
from typing import Any

import pytest

from asyncmq.backends.memory import InMemoryBackend
from asyncmq.conf import settings


def custom_json_encoder(obj: Any) -> Any:
    """Custom encoder that handles UUID objects."""
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def custom_json_decoder(dct: dict[str, Any]) -> dict[str, Any]:
    """Custom decoder that converts UUID strings back to UUID objects."""
    for key, value in dct.items():
        if isinstance(value, str) and key.endswith('_id'):
            try:
                # Try to parse as UUID if the key suggests it's an ID
                dct[key] = uuid.UUID(value)
            except ValueError:
                # Not a valid UUID, keep as string
                pass
    return dct


def test_default_json_functions():
    """Test that default JSON functions are standard library functions."""
    # Reset to defaults
    settings.json_dumps = json.dumps
    settings.json_loads = json.loads

    assert settings.json_dumps == json.dumps
    assert settings.json_loads == json.loads

def test_can_configure_custom_json_parser():
    custom_dumps = partial(json.dumps, default=custom_json_encoder)
    custom_loads = partial(json.loads, object_hook=custom_json_decoder)

    settings.json_dumps = custom_dumps
    settings.json_loads = custom_loads

    assert settings.json_dumps == custom_dumps
    assert settings.json_loads == custom_loads

class TestBaseBackend:
    @pytest.fixture(autouse=True)
    def _configure_json(self):
        original_dumps = settings.json_dumps
        original_loads = settings.json_loads
        settings.json_dumps = partial(json.dumps, default=custom_json_encoder)
        settings.json_loads = partial(json.loads, object_hook=custom_json_decoder)

        yield

        settings.json_dumps = original_dumps
        settings.json_loads = original_loads


    def test_serialize_json_with_uuid(self):
        backend = InMemoryBackend()

        test_uuid = uuid.uuid4()
        data = {"user_id": test_uuid, "task": "process_user"}

        json_str = backend._json_serializer.to_json(data)

        assert isinstance(json_str, str)
        assert str(test_uuid) in json_str

    def test_deserialize_json_with_uuid(self):
        backend = InMemoryBackend()

        test_uuid = uuid.uuid4()
        json_str = f'{{"user_id": "{test_uuid}", "task": "process_user"}}'

        data = backend._json_serializer.to_dict(json_str)

        assert isinstance(data["user_id"], uuid.UUID)
        assert data["user_id"] == test_uuid
        assert data["task"] == "process_user"

    def test_round_trip_serialization(self):
        backend = InMemoryBackend()

        original_data = {
            "id": "job-123",
            "user_id": uuid.uuid4(),
            "session_id": uuid.uuid4(),
            "task": "process_data",
            "metadata": {
                "correlation_id": uuid.uuid4(),
                "timestamp": 1234567890
            }
        }

        json_str = backend._json_serializer.to_json(original_data)
        restored_data = backend._json_serializer.to_dict(json_str)

        # Check that UUIDs are preserved
        assert isinstance(restored_data["user_id"], uuid.UUID)
        assert isinstance(restored_data["session_id"], uuid.UUID)
        assert isinstance(restored_data["metadata"]["correlation_id"], uuid.UUID)

        # Check that values are equal
        assert restored_data["user_id"] == original_data["user_id"]
        assert restored_data["session_id"] == original_data["session_id"]
        assert restored_data["metadata"]["correlation_id"] == original_data["metadata"]["correlation_id"]
        assert restored_data["task"] == original_data["task"]
        assert restored_data["metadata"]["timestamp"] == original_data["metadata"]["timestamp"]


    @pytest.mark.asyncio
    async def test_memory_backend_with_uuid_jobs(self):
        backend = InMemoryBackend()

        test_uuid = uuid.uuid4()
        job_payload = {
            "id": "test-job-1",
            "user_id": test_uuid,
            "task": "process_user_data",
            "metadata": {"session_id": uuid.uuid4()}
        }

        await backend.enqueue("test_queue", job_payload)
        dequeued_job = await backend.dequeue("test_queue")

        assert isinstance(dequeued_job["user_id"], uuid.UUID)
        assert isinstance(dequeued_job["metadata"]["session_id"], uuid.UUID)
        assert dequeued_job["user_id"] == test_uuid
        assert dequeued_job["task"] == "process_user_data"
