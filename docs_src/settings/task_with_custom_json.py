"""
Example showing how to use custom JSON encoding with AsyncMQ tasks.

This demonstrates the complete workflow from task definition to execution
with custom data types being properly serialized and deserialized.
"""

import asyncio
import json
import uuid
from datetime import datetime
from decimal import Decimal
from functools import partial

from asyncmq import Queue, task
from asyncmq.conf.global_settings import Settings as BaseSettings
from asyncmq.backends.memory import InMemoryBackend


def custom_json_encoder(obj):
    """Custom encoder that handles UUID, datetime, and Decimal objects."""
    if isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return str(obj)  # Preserve precision
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
            # Try to parse Decimal amounts
            elif key in ['amount', 'price', 'total', 'balance']:
                try:
                    dct[key] = Decimal(value)
                except (ValueError, TypeError):
                    pass
    return dct


class Settings(BaseSettings):
    """Settings with custom JSON encoding for the example."""
    json_dumps = partial(json.dumps, default=custom_json_encoder)
    json_loads = partial(json.loads, object_hook=custom_json_decoder)
    backend = InMemoryBackend()  # Use in-memory backend for this example


# Define tasks that work with custom data types
@task
async def process_payment(payment_data):
    """Process a payment with UUID, datetime, and Decimal data."""
    print(f"Processing payment {payment_data['payment_id']}")
    print(f"User: {payment_data['user_id']} (type: {type(payment_data['user_id'])})")
    print(f"Amount: ${payment_data['amount']} (type: {type(payment_data['amount'])})")
    print(f"Created: {payment_data['created_at']} (type: {type(payment_data['created_at'])})")

    # Simulate payment processing
    await asyncio.sleep(0.1)

    # Return result with custom types
    return {
        "transaction_id": uuid.uuid4(),
        "processed_at": datetime.now(),
        "status": "completed",
        "fee": Decimal("2.50")
    }


@task
async def send_notification(user_id, message, scheduled_at=None):
    """Send a notification to a user."""
    print(f"Sending notification to user {user_id} (type: {type(user_id)})")
    print(f"Message: {message}")
    if scheduled_at:
        print(f"Scheduled for: {scheduled_at} (type: {type(scheduled_at)})")

    await asyncio.sleep(0.1)
    return {"sent_at": datetime.now(), "notification_id": uuid.uuid4()}


async def main():
    """Demonstrate custom JSON encoding with AsyncMQ tasks."""
    # Create queue
    queue = Queue("payments", backend=InMemoryBackend())

    # Sample payment data with custom types
    payment_data = {
        "payment_id": uuid.uuid4(),
        "user_id": uuid.uuid4(),
        "amount": Decimal("99.99"),
        "created_at": datetime.now(),
        "method": "credit_card"
    }

    print("=== Enqueueing Payment Job ===")
    print(f"Payment ID: {payment_data['payment_id']}")
    print(f"Amount: ${payment_data['amount']} (type: {type(payment_data['amount'])})")

    # Enqueue the payment processing job
    job = await queue.enqueue(process_payment, payment_data)
    print(f"Job enqueued with ID: {job.id}")

    # Process the job
    print("\n=== Processing Job ===")
    result = await process_payment(payment_data)
    print(f"Result: {result}")
    print(f"Transaction ID: {result['transaction_id']} (type: {type(result['transaction_id'])})")
    print(f"Fee: ${result['fee']} (type: {type(result['fee'])})")

    # Enqueue notification with UUID and datetime
    print("\n=== Enqueueing Notification Job ===")
    notification_queue = Queue("notifications", backend=InMemoryBackend())
    notification_job = await notification_queue.enqueue(
        send_notification,
        payment_data['user_id'],
        f"Payment of ${payment_data['amount']} processed successfully",
        datetime.now()
    )
    print(f"Notification job enqueued with ID: {notification_job.id}")


if __name__ == "__main__":
    # Set up the custom settings
    import asyncmq.conf
    asyncmq.conf.settings = Settings()

    # Run the example
    asyncio.run(main())
