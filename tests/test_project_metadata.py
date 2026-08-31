import re
from pathlib import Path


def test_all_extra_uses_concrete_dependencies() -> None:
    """The `all` optional dependency must install real packages, not extra names."""
    text = Path("pyproject.toml").read_text(encoding="utf-8")

    opt_section = re.search(r"\[project\.optional-dependencies\](.*?)\n\[", text, re.S)
    assert opt_section is not None

    mongo_entry = re.search(r"\nmongo\s*=\s*\[(.*?)\]\n", opt_section.group(1), re.S)
    all_entry = re.search(r"\nall\s*=\s*\[(.*?)\]\n", opt_section.group(1), re.S)
    assert mongo_entry is not None
    assert all_entry is not None

    assert "pymongo>=4.13.0" in mongo_entry.group(1)
    body = all_entry.group(1)
    assert "asyncpg" in body
    assert "pymongo>=4.13.0" in body
    assert "aio_pika" in body
    assert "motor" not in opt_section.group(1)

    # Guard against accidentally using extra names (which are not installable packages).
    assert '"postgres"' not in body
    assert '"mongo"' not in body
    assert '"aio-pika"' not in body
