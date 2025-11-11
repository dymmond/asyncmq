from __future__ import annotations

from typing import Any, Protocol

from lilya.requests import Request
from lilya.responses import Response


class User:
    """
    Represents an authenticated user within the Asyncz Admin dashboard context.

    Instances of this class are typically created by the `AuthBackend` upon successful
    authentication and attached to the request state.
    """

    def __init__(self, id: str | int, name: str, is_admin: bool = True, **extra: Any):
        """
        Initializes the User object.

        Args:
            id: The unique identifier for the user.
            name: The display name of the user.
            is_admin: Boolean indicating if the user has administrative privileges (default: True).
            **extra: Additional, arbitrary user data to store (e.g., email, roles).
        """
        self.id: str | int = id
        self.name: str = name
        self.is_admin: bool = is_admin
        self.extra: dict[str, Any] = extra


class AuthBackend(Protocol):
    """
    Protocol defining the interface for an authentication backend used by the
    Asyncz Admin dashboard.

    A concrete implementation (e.g., SimpleUsernamePasswordBackend) must conform
    to these asynchronous methods to manage user session lifecycle and verification.
    """

    async def authenticate(self, request: Request) -> User | None:
        """
        Verifies the user's current credentials (e.g., by checking session cookies or headers).

        This method is executed by the authentication middleware on every request.

        Args:
            request: The incoming request object.

        Returns:
            The authenticated `User` object if valid credentials are found, otherwise `None`.
        """
        ...

    async def login(self, request: Request) -> Response:
        """
        Handles the logic for the `/login` path, typically processing credentials
        from a POST request and setting the session/cookie upon success.

        Args:
            request: The incoming request object.

        Returns:
            A `Response` object, usually an HTML form (GET/failure) or a redirect (success).
        """
        ...

    async def logout(self, request: Request) -> Response:
        """
        Handles the logic for the `/logout` path, clearing the user's session state.

        Args:
            request: The incoming request object.

        Returns:
            A `Response` object, usually a redirect to the login page.
        """
        ...

    def routes(self) -> list[Any]:
        """
        An optional method to return extra routing definitions (e.g., the GET endpoint
        for displaying the login form) that the backend requires.

        Returns:
            A list of route definitions (e.g., Lilya `Path` or `Include` objects).
        """
        ...
