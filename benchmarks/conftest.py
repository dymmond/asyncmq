import pytest

from asyncmq.backends.memory import InMemoryBackend


@pytest.fixture
def memory_backend():
    """Provide a fresh InMemoryBackend for each benchmark."""
    return InMemoryBackend()
