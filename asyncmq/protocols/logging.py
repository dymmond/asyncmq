from typing import Any, runtime_checkable

from typing_extensions import Protocol


@runtime_checkable
class LoggerProtocol(Protocol):
    """
    Defines the expected interface for a logger object used within asyncmq.

    This Protocol specifies the standard logging methods (debug, info, warning,
    error, critical) that any compatible logger must implement. This allows
    different logging libraries (like standard logging, loguru, structlog)
    to be used interchangeably as long as they adhere to this contract.
    The `@runtime_checkable` decorator allows using `isinstance()` with this
    Protocol.
    """

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """
        Logs a message with level DEBUG.

        Args:
            msg: The message string.
            *args: Positional arguments for the message.
            **kwargs: Keyword arguments, potentially including 'exc_info' for exceptions.
        """
        ...

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """
        Logs a message with level INFO.

        Args:
            msg: The message string.
            *args: Positional arguments for the message.
            **kwargs: Keyword arguments.
        """
        ...

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """
        Logs a message with level WARNING.

        Args:
            msg: The message string.
            *args: Positional arguments for the message.
            **kwargs: Keyword arguments.
        """
        ...

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """
        Logs a message with level ERROR.

        Args:
            msg: The message string.
            *args: Positional arguments for the message.
            **kwargs: Keyword arguments.
        """
        ...

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        """
        Logs a message with level CRITICAL.

        Args:
            msg: The message string.
            *args: Positional arguments for the message.
            **kwargs: Keyword arguments.
        """
        ...
