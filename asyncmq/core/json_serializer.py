"""
JSON serialization utilities for AsyncMQ.

This module provides a centralized way to handle JSON serialization and deserialization
with support for custom encoders and decoders, including proper handling of partial functions.
"""

from typing import Any, Callable, cast


class JSONSerializer:
    """
    A JSON serializer that handles both regular functions and partial functions.

    This class encapsulates the logic for calling JSON dumps/loads functions correctly,
    whether they are regular functions, partial functions, or other callables.
    """

    def __init__(self, json_dumps: Callable[[Any], str], json_loads: Callable[[str], Any]) -> None:
        """
        Initialize the JSON serializer with custom dump and load functions.

        Args:
            json_dumps: Function to serialize objects to JSON strings
            json_loads: Function to deserialize JSON strings to objects
        """
        self._json_dumps = json_dumps
        self._json_loads = json_loads

    def to_json(self, obj: Any) -> str:
        """
        Serialize an object to JSON using the configured json_dumps function.

        This method handles both regular functions and partial functions correctly
        by checking for the __func__ attribute and using the appropriate calling convention.

        Args:
            obj: The object to serialize to JSON

        Returns:
            A JSON string representation of the object

        Raises:
            TypeError: If the object cannot be serialized to JSON
        """
        try:
            # Try to get the underlying function for partial functions
            dumps = self._json_dumps.__func__  # noqa
        except AttributeError:
            # Not a partial function, use directly
            dumps = self._json_dumps
        return cast(str, dumps(obj))

    def to_dict(self, json_str: str) -> Any:
        """
        Deserialize a JSON string using the configured json_loads function.

        This method handles both regular functions and partial functions correctly
        by checking for the __func__ attribute and using the appropriate calling convention.

        Args:
            json_str: The JSON string to deserialize

        Returns:
            The deserialized Python object

        Raises:
            ValueError: If the JSON string is malformed
            TypeError: If the JSON cannot be deserialized
        """
        try:
            # Try to get the underlying function for partial functions
            loads = self._json_loads.__func__  # noqa
        except AttributeError:
            # Not a partial function, use directly
            loads = self._json_loads
        return loads(json_str)

    def __repr__(self) -> str:
        """Return a string representation of the JSON serializer."""
        return f"JSONSerializer(dumps={self._json_dumps!r}, loads={self._json_loads!r})"
