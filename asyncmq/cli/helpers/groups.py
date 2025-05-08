import sys
from typing import Any, TypeVar

import click

from asyncmq.cli.helpers.env import AsyncMQEnv

T = TypeVar("T")


class AsyncMQGroup(click.Group):
    """
    A custom click.Group that ensures the AsyncMQ environment settings
    are enabled before invoking any command within the group.
    """

    def invoke(self, ctx: click.Context) -> Any:
        """
        Invokes the command, enabling AsyncMQ settings beforehand.

        Attempts to enable the AsyncMQ environment settings. If this fails,
        the program exits with status 1. Otherwise, it proceeds to invoke
        the standard click.Group.invoke method.

        Args:
            ctx: The click.Context object representing the current invocation.

        Returns:
            The result returned by the parent click.Group.invoke method.
        """
        asyncmq_env = AsyncMQEnv()
        try:
            asyncmq_env.enable_settings()
        except Exception:  # noqa
            sys.exit(1)
        result = super().invoke(ctx)
        return result
