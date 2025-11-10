from __future__ import annotations

from collections.abc import Callable
from typing import Any

from lilya.datastructures import FormData
from lilya.requests import Request
from lilya.responses import PlainText, RedirectResponse, Response

from asyncmq.contrib.dashboard.admin.protocols import AuthBackend, User
from asyncmq.contrib.dashboard.engine import templates
from asyncmq.contrib.dashboard.mixins import default_context

VerifyCallable = Callable[[str, str], User | None]


class SimpleUsernamePasswordBackend(AuthBackend):
    """
    A basic authentication backend that verifies credentials using a synchronous
    user-provided callable and manages user state via the session (cookies).

    This backend implements the `AuthBackend` protocol for the AsyncMQ dashboard,
    handling the entire login/logout flow and session state management.
    """

    def __init__(
        self,
        verify: VerifyCallable,
        session_key: str = "asyncmq_admin",
        login_template: str = "login.html",
    ) -> None:
        """
        Initializes the simple username/password backend.

        Args:
            verify: A synchronous callable that takes (username: str, password: str)
                    and returns a populated `User` object if credentials are valid, or `None` otherwise.
            session_key: The key used to store user data in the request session.
                         Defaults to "asyncmq_admin".
            login_template: The name of the template file to render for the login page.
                         Defaults to "login.html".
        """
        self.verify: VerifyCallable = verify
        self.session_key: str = session_key
        self.login_template: str = login_template

    async def authenticate(self, request: Request) -> User | None:
        """
        Checks if the user is currently authenticated by looking up the session key.

        Args:
            request: The incoming request object containing the session data.

        Returns:
            The authenticated `User` object if found in the session, or `None`.
        """
        data: dict[str, Any] | None = request.session.get(self.session_key)
        # Reconstruct the User object from the session data
        return User(**data) if data else None

    async def login(self, request: Request) -> Response:
        """
        Handles both GET (rendering the login form) and POST (processing credentials) requests.

        On successful POST, it saves user details to the session and redirects to the next page.

        Args:
            request: The incoming request object.

        Returns:
            An `HTMLResponse` containing the login form (GET/failure) or a
            `RedirectResponse` (success).
        """
        context: dict[str, Any] = default_context(request)

        if request.method == "GET":
            # Render login form (initial load)
            context.update({"next": request.query_params.get("next")})
            return templates.get_template_response(
                request,
                self.login_template,
                context,
            )

        form: FormData = await request.form()
        username: str = (form.get("username") or "").strip()
        password: str = form.get("password") or ""

        # Verify credentials using the synchronous callable
        user: User | None = self.verify(username, password)

        if not user:
            # Login failed: re-render form with error message
            context.update({"error": "Invalid credentials"})
            return templates.get_template_response(request, self.login_template, context)

        # Login successful: store user data in session
        request.session[self.session_key] = {
            "id": user.id,
            "name": user.name,
            "is_admin": user.is_admin,
            **user.extra,  # Merge any extra user data
        }

        # Redirect to the requested 'next' URL or default to mount root
        root: str = request.scope.get("root_path", "") or ""
        nxt: str = form.get("next") or (root + "/")
        return RedirectResponse(nxt, status_code=303)

    async def logout(self, request: Request) -> Response:
        """
        Handles user logout by clearing the session key and redirecting to the login page.

        Args:
            request: The incoming request object.

        Returns:
            A `RedirectResponse` to the mount-aware `/login` path or an HTMX-compatible response.
        """
        # Remove user data from the session
        request.session.pop(self.session_key, None)

        # Compute mount-aware login URL
        root: str = request.scope.get("root_path", "") or ""
        login_url: str = f"{root}/login"

        # HTMX-friendly redirect
        if request.headers.get("hx-request") == "true":
            return PlainText("Logged out", status_code=204, headers={"HX-Redirect": login_url})

        # Standard redirect
        return RedirectResponse(login_url, status_code=303)
