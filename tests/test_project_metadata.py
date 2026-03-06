import re
from pathlib import Path


def test_all_extra_uses_concrete_dependencies() -> None:
    """The `all` optional dependency must install real packages, not extra names."""
    text = Path("pyproject.toml").read_text(encoding="utf-8")

    opt_section = re.search(r"\[project\.optional-dependencies\](.*?)\n\[", text, re.S)
    assert opt_section is not None

    all_entry = re.search(r"\nall\s*=\s*\[(.*?)\]\n", opt_section.group(1), re.S)
    assert all_entry is not None

    body = all_entry.group(1)
    assert "asyncpg" in body
    assert "motor" in body
    assert "aio_pika" in body

    # Guard against accidentally using extra names (which are not installable packages).
    assert '"postgres"' not in body
    assert '"mongo"' not in body
    assert '"aio-pika"' not in body
