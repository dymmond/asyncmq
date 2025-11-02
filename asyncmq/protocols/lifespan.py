from typing import Any, Awaitable, Protocol, runtime_checkable


@runtime_checkable
class Lifespan(Protocol):
    """
    A Protocol defining the interface for an asynchronous application lifespan hook.

    This is used by frameworks (like Starlette or Lilya or AsyncMQ) to define functions
    that should run during the application's startup and shutdown phases.

    The `@runtime_checkable` decorator allows this protocol to be used with
    `isinstance()` checks at runtime.
    """

    def __call__(self, *args: Any, **kwargs: Any) -> Awaitable[None] | None:
        """
        The required callable signature for the lifespan hook.

        The implementation should be an `async` function (returning `Awaitable[None]`)
        or a synchronous function that is not expected to be awaited (returning `None`).
        In ASGI contexts, these hooks are typically executed via the lifespan protocol.

        Args:
            *args: Positional arguments passed to the hook (e.g., application instance).
            **kwargs: Keyword arguments passed to the hook.

        Returns:
            An awaitable object (Coroutine) that resolves to `None` for async functions,
            or `None` for synchronous functions.
        """
        ...
