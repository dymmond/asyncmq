"""
Real-world example: E-commerce order processing with custom JSON encoding.

This example demonstrates how to configure AsyncMQ for an e-commerce system
that needs to handle UUIDs, timestamps, and monetary amounts with precision.
"""

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
    """E-commerce settings with custom JSON handling."""
    json_dumps = partial(json.dumps, default=ecommerce_json_encoder)
    json_loads = partial(json.loads, object_hook=ecommerce_json_decoder)


# Example order data
def create_sample_order():
    """Create a sample order with various data types."""
    return {
        "order_id": uuid.uuid4(),
        "user_id": uuid.uuid4(),
        "session_id": uuid.uuid4(),
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "total": Decimal("199.98"),
        "tax": Decimal("16.00"),
        "shipping_cost": Decimal("9.99"),
        "status": "pending",
        "items": [
            {
                "product_id": uuid.uuid4(),
                "name": "Wireless Headphones",
                "price": Decimal("99.99"),
                "quantity": 1
            },
            {
                "product_id": uuid.uuid4(),
                "name": "Phone Case",
                "price": Decimal("99.99"),
                "quantity": 1
            }
        ],
        "shipping_address": {
            "street": "123 Main St",
            "city": "Anytown",
            "zip": "12345"
        }
    }


if __name__ == "__main__":
    settings = Settings()

    # Create sample order
    order = create_sample_order()

    print("Original order data:")
    print(f"Order ID: {order['order_id']} (type: {type(order['order_id'])})")
    print(f"Total: ${order['total']} (type: {type(order['total'])})")
    print(f"Created: {order['created_at']} (type: {type(order['created_at'])})")
    print(f"First item price: ${order['items'][0]['price']} (type: {type(order['items'][0]['price'])})")

    # Serialize to JSON
    json_str = settings.json_dumps(order)
    print(f"\nSerialized JSON (first 200 chars):\n{json_str[:200]}...")

    # Deserialize from JSON
    restored_order = settings.json_loads(json_str)

    print(f"\nRestored order data:")
    print(f"Order ID: {restored_order['order_id']} (type: {type(restored_order['order_id'])})")
    print(f"Total: ${restored_order['total']} (type: {type(restored_order['total'])})")
    print(f"Created: {restored_order['created_at']} (type: {type(restored_order['created_at'])})")
    print(f"First item price: ${restored_order['items'][0]['price']} (type: {type(restored_order['items'][0]['price'])})")

    # Verify data integrity
    assert restored_order["order_id"] == order["order_id"]
    assert restored_order["total"] == order["total"]
    assert restored_order["created_at"] == order["created_at"]
    assert restored_order["items"][0]["price"] == order["items"][0]["price"]

    print("\n✅ E-commerce order serialization successful!")
    print("💰 Decimal precision preserved for financial calculations")
