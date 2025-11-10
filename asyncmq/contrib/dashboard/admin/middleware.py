from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from lilya.requests import Request
from lilya.responses import PlainText, RedirectResponse, Response
from lilya.types import ASGIApp, Receive, Scope, Send

AuthenticateCallable = Callable[[Request], Awaitable[Any | None]]


class AuthGateMiddleware:
    """
    ASGI middleware that enforces authentication for the wrapped child application,
    selectively bypassing the check for configured allowlisted paths.

    The middleware is designed to be HTMX-friendly: for HTMX requests, it returns a
    401 Unauthorized response with the **HX-Redirect** header, prompting the frontend
    to navigate to the login page without disrupting the main page navigation.
    It correctly handles mounted applications by normalizing paths relative to the child app.
    """

    def __init__(
        self,
        app: ASGIApp,
        authenticate: AuthenticateCallable,
        login_path: str = "/login",
        allowlist: Iterable[str] = ("/login", "/logout", "/static", "/assets"),
    ) -> None:
        """
        Initializes the AuthGateMiddleware.

        Args:
            app: The next ASGI application to call.
            authenticate: An asynchronous callable (`request -> User | None`) that verifies
                          credentials (e.g., checks session or token).
            login_path: The path *relative to the child app's root* to redirect to for login.
                        Defaults to "/login".
            allowlist: An iterable of paths (relative to the child app's root) that do not
                       require authentication. Defaults include login/logout/static assets.
        """
        self.app: ASGIApp = app
        self.authenticate: AuthenticateCallable = authenticate
        self.login_path: str = login_path
        self.allowlist: tuple[str, ...] = tuple(allowlist)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """
        The ASGI entry point for the middleware.

        Args:
            scope: The ASGI scope dictionary.
            receive: The ASGI receive callable.
            send: The ASGI send callable.
        """
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        # 1. Path Normalization (Handling Mounts)
        mount_prefix: str = scope.get("root_path", "") or ""
        path: str = scope.get("path") or "/"

        # Normalize to child-relative path by stripping the mount prefix
        child_path: str = path
        if mount_prefix and path.startswith(mount_prefix):
            child_path = path[len(mount_prefix) :] or "/"
        if not child_path.startswith("/"):
            child_path = "/" + child_path

        # 2. Allow-list Check
        # Check if the child_path matches an exact path or starts with a path prefix followed by a slash.
        if any(child_path == p or child_path.startswith(p + "/") for p in self.allowlist):
            return await self.app(scope, receive, send)

        # 3. Authentication
        request: Request = Request(scope, receive=receive)

        user: Any | None = await self.authenticate(request)
        if user:
            # Attach user to request state and proceed
            request.state.user = user
            # Must call the app using the original scope/receive/send, not the Request object
            return await self.app(request.scope, receive, send)

        # 4. Unauthenticated Response
        # Build the final login URL under the mount prefix
        login_url: str = f"{mount_prefix}{self.login_path}"

        # Determine the target URL after login
        next_q: str = f"?next={path}"

        response: Response
        if request.headers.get("hx-request") == "true":
            # HTMX request: Respond with 401 and HX-Redirect header
            response = PlainText(
                "Authentication required",
                status_code=401,
                headers={"HX-Redirect": login_url + next_q},
            )
        else:
            # Standard request: Redirect to login page
            response = RedirectResponse(login_url + next_q, status_code=303)

        # Send the response through the ASGI pipeline
        await response(scope, receive, send)
