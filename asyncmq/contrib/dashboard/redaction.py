from __future__ import annotations

import json
import re
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
TEXT_SECRET_PATTERN = re.compile(
    r"\b("
    r"authorization|api[_-]?key|apikey|credential|password|private[_-]?key|secret|token"
    r")\b([\"']?\s*[:=]\s*)(\\?[\"']?)([^\\\"'\s,;&)\]}]+)(\\?[\"']?)",
    re.IGNORECASE,
)


def is_sensitive_key(key: str) -> bool:
    """Return whether a metadata key should be redacted before display."""
    lowered = key.lower()
    return any(part in lowered for part in SENSITIVE_KEY_PARTS)


def redact_text_for_display(value: str) -> str:
    """Redact common inline secret assignments from text shown to operators."""

    def replace_secret(match: re.Match[str]) -> str:
        opening_quote = match.group(3)
        closing_quote = match.group(5) if opening_quote and match.group(5) == opening_quote else ""
        return f"{match.group(1)}{match.group(2)}{opening_quote}[redacted]{closing_quote}"

    redacted = TEXT_SECRET_PATTERN.sub(replace_secret, value)
    if len(redacted) > MAX_DISPLAY_STRING_LENGTH:
        return f"{redacted[:MAX_DISPLAY_STRING_LENGTH]}...[truncated]"
    return redacted


def redact_for_display(value: Any, *, depth: int = 0) -> Any:
    """Return a bounded, redacted copy of runtime data shown to operators."""
    if depth > MAX_REDACTION_DEPTH:
        return "[nested data omitted]"
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            text_key = str(key)
            redacted[text_key] = (
                "[redacted]"
                if is_sensitive_key(text_key)
                else redact_for_display(
                    item,
                    depth=depth + 1,
                )
            )
        return redacted
    if isinstance(value, list):
        return [redact_for_display(item, depth=depth + 1) for item in value]
    if isinstance(value, tuple):
        return [redact_for_display(item, depth=depth + 1) for item in value]
    if isinstance(value, str):
        return redact_text_for_display(value)
    return value


def to_pretty_json(value: Any) -> str:
    """Serialize redacted display data as stable indented JSON."""
    return json.dumps(
        redact_for_display(value),
        indent=2,
        sort_keys=True,
        default=str,
    )
