from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Annotated, Any, ClassVar
from asyncmq.backends.base import BaseBackend
from asyncmq.backends.redis import RedisBackend

from asyncmq import __version__


@dataclass
class _BaseSettings:
    """Base class for settings."""

    def dict(self, exclude_none: bool = False, upper: bool = False) -> dict[str, Any]:
        """
        Dumps all the settings into a python dictionary.
        """
        original = asdict(self)

        if not exclude_none:
            if not upper:
                return original
            return {k.upper(): v for k, v in original.items()}

        if not upper:
            return {k: v for k, v in original.items() if v is not None}
        return {k.upper(): v for k, v in original.items() if v is not None}

    def tuple(self, exclude_none: bool = False, upper: bool = False) -> list[tuple[str, Any]]:
        """
        Dumps all the settings into a tuple.
        """
        original = asdict(self)

        if not exclude_none:
            if not upper:
                return list(original.items())
            return list({k.upper(): v for k, v in original.items()}.items())

        if not upper:
            return [(k, v) for k, v in original.items() if v is not None]
        return [(k.upper(), v) for k, v in original.items() if v is not None]

@dataclass
class Settings(_BaseSettings):
    debug: bool = False
    """
    Tells if the application is running in debug mode.
    """
    backend: type[BaseBackend] = RedisBackend()
    """
    Default backend to use for queue operations.
    """
    version: str = __version__
    """
    The version of AsyncMQ.
    """
