from __future__ import annotations

import json
from typing import Any

SENSITIVE_KEY_PARTS: tuple[str, ...] = (
    "authorization",
    "api_key",
    "apikey",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)
MAX_DISPLAY_STRING_LENGTH = 4000
MAX_REDACTION_DEPTH = 8


def is_sensitive_key(key: str) -> bool:
    """Return whether a metadata key should be redacted before display."""
    lowered = key.lower()
    return any(part in lowered for part in SENSITIVE_KEY_PARTS)


def redact_for_display(value: Any, *, depth: int = 0) -> Any:
    """Return a bounded, redacted copy of operator-visible runtime data."""
    if depth > MAX_REDACTION_DEPTH:
        return "[nested data omitted]"
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            text_key = str(key)
            redacted[text_key] = "[redacted]" if is_sensitive_key(text_key) else redact_for_display(
                item,
                depth=depth + 1,
            )
        return redacted
    if isinstance(value, list):
        return [redact_for_display(item, depth=depth + 1) for item in value]
    if isinstance(value, tuple):
        return [redact_for_display(item, depth=depth + 1) for item in value]
    if isinstance(value, str) and len(value) > MAX_DISPLAY_STRING_LENGTH:
        return f"{value[:MAX_DISPLAY_STRING_LENGTH]}...[truncated]"
    return value


def to_pretty_json(value: Any) -> str:
    """Serialize redacted display data as stable indented JSON."""
    return json.dumps(
        redact_for_display(value),
        indent=2,
        sort_keys=True,
        default=str,
    )
