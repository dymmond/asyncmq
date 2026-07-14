from __future__ import annotations

from typing import cast

from lilya.apps import ChildLilya, Lilya
from lilya.controllers import Controller
from lilya.middleware.base import DefineMiddleware
from lilya.middleware.cors import CORSMiddleware
from lilya.middleware.security import SecurityMiddleware
from lilya.requests import Request
from lilya.responses import Response
from lilya.routing import Include, Path
from lilya.types import ASGIApp

from asyncmq import monkay
from asyncmq.contrib.dashboard import create_dashboard_app
from asyncmq.contrib.dashboard.admin.middleware import AuthGateMiddleware
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend
from asyncmq.contrib.dashboard.security import (
    DASHBOARD_CONTENT_SECURITY_POLICY,
    ContentSecurityPolicy,
)


class AsyncMQAdmin:
    """
    A configurable wrapper for the AsyncMQ web dashboard and management API.

    This class handles the creation of a private `ChildLilya` application, configures
    CORS, session management, and optional authentication (via AuthGateMiddleware),
    and exposes a method to mount itself onto a parent Lilya application.
    """

    def __init__(
        self,
        enable_login: bool = False,
        backend: AuthBackend | None = None,
        url_prefix: str | None = None,
        include_security: bool = True,
        content_security_policy: ContentSecurityPolicy | None = None,
        include_session: bool = True,
        include_cors: bool = True,
        cors_allow_origins: tuple[str, ...] | None = None,
        cors_allow_methods: tuple[str, ...] | None = None,
        cors_allow_headers: tuple[str, ...] | None = None,
        cors_allow_credentials: bool | None = None,
        enforce_same_origin: bool = True,
        require_admin: bool = True,
        required_roles: tuple[str, ...] = (),
        trusted_proxies: tuple[str, ...] | None = None,
        login_path: str = "/login",
        allowlist: tuple[str, ...] = ("/login", "/logout", "/static", "/assets"),
    ) -> None:
        """
        Initializes the AsyncMQ Admin dashboard instance.

        Args:
            enable_login: If True, enables session and authentication middleware, requiring a `backend`.
            backend: The authentication backend implementing `AuthBackend` methods (required if `enable_login` is True).
            url_prefix: The base URL path where the dashboard should be mounted (e.g., "/asyncmq").
                        Defaults to the value from `monkay.settings.dashboard_config`.
            include_security: If True, add dashboard security headers,
                        including a strict Content Security Policy.
            content_security_policy: Optional CSP override. Defaults to the
                        packaged dashboard policy when `include_security` is True.
            enforce_same_origin: If True, reject authenticated unsafe requests
                        whose Origin is missing or does not match the dashboard host.
            require_admin: If True, authenticated users must expose admin
                        privileges before dashboard access is allowed.
            required_roles: Optional role names; when provided, authenticated
                        users must have at least one of these roles.
            trusted_proxies: Optional client peer addresses whose forwarded
                        host/proto headers may participate in checks for the same origin.

        Raises:
            ValueError: If `enable_login` is True but no `backend` is provided.
        """
        if enable_login and not backend:
            raise ValueError("`backend` must not be `None` when enable login is True")

        # Resolve defaults
        config = monkay.settings.dashboard_config
        self.url_prefix: str = (url_prefix or config.dashboard_url_prefix).rstrip("/")
        self.enable_login: bool = enable_login
        self.backend: AuthBackend = cast(AuthBackend, backend)

        # Extras
        self.include_security = include_security
        self.content_security_policy = (
            content_security_policy if content_security_policy is not None else DASHBOARD_CONTENT_SECURITY_POLICY
        )
        self.include_session = include_session
        self.include_cors = include_cors
        self.cors_allow_origins = cors_allow_origins
        self.cors_allow_methods = cors_allow_methods
        self.cors_allow_headers = cors_allow_headers
        self.cors_allow_credentials = cors_allow_credentials
        self.enforce_same_origin = enforce_same_origin
        self.require_admin = require_admin
        self.required_roles = required_roles
        self.trusted_proxies = trusted_proxies if trusted_proxies is not None else config.trusted_proxies
        self.login_path = login_path
        self.allowlist = allowlist

        # Build the internal ChildLilya application immediately
        self.child_app: ChildLilya = self._build_child()

    def _build_child(self) -> ChildLilya:
        """
        Constructs the internal `ChildLilya` application with all necessary middlewares
        and routes (dashboard, login/logout).

        Returns:
            The fully configured `ChildLilya` application instance.
        """
        config = monkay.settings.dashboard_config
        middlewares: list[DefineMiddleware] = []

        if self.include_security:
            middlewares.append(
                DefineMiddleware(
                    SecurityMiddleware,
                    content_policy=self.content_security_policy,
                )
            )

        if self.include_cors:
            allow_origins = self.cors_allow_origins or config.cors_allow_origins
            allow_methods = self.cors_allow_methods or config.cors_allow_methods
            allow_headers = self.cors_allow_headers or config.cors_allow_headers
            allow_credentials = (
                self.cors_allow_credentials
                if self.cors_allow_credentials is not None
                else config.cors_allow_credentials
            )
            if "*" in allow_origins and allow_credentials:
                raise ValueError("Dashboard CORS cannot combine wildcard origins with credentials.")
            if allow_origins:
                middlewares.append(
                    DefineMiddleware(
                        CORSMiddleware,
                        allow_origins=list(allow_origins),
                        allow_methods=list(allow_methods),
                        allow_headers=list(allow_headers),
                        allow_credentials=allow_credentials,
                    )
                )

        if self.include_session:
            middlewares.append(config.session_middleware)

        if self.enable_login:
            # Append AuthGateMiddleware if login is enabled
            middlewares.append(
                DefineMiddleware(
                    AuthGateMiddleware,
                    authenticate=self.backend.authenticate,
                    login_path=self.login_path,
                    allowlist=self.allowlist,
                    enforce_same_origin=self.enforce_same_origin,
                    require_admin=self.require_admin,
                    required_roles=self.required_roles,
                    trusted_proxies=self.trusted_proxies,
                )
            )

        # 2. Route Setup (Login/Logout, Dashboard)
        routes: list[Path | Include] = []
        if self.enable_login:
            auth_backend = self.backend

            class LoginController(Controller):
                """Delegates dashboard login requests to the configured authentication backend."""

                async def get(self, request: Request) -> Response:
                    """Render the backend-owned login response for GET requests."""
                    return await auth_backend.login(request)

                async def post(self, request: Request) -> Response:
                    """Process backend-owned login submissions for POST requests."""
                    return await auth_backend.login(request)

            class LogoutController(Controller):
                """Delegates dashboard logout requests to the configured authentication backend."""

                async def get(self, request: Request) -> Response:
                    """Process backend-owned logout requests submitted with GET."""
                    return await auth_backend.logout(request)

                async def post(self, request: Request) -> Response:
                    """Process backend-owned logout requests submitted with POST."""
                    return await auth_backend.logout(request)

            login_logout: list[Path] = [
                Path("/login", LoginController, methods=["GET", "POST"]),
                Path("/logout", LogoutController, methods=["GET", "POST"]),
            ]
            routes.extend(login_logout)

        # Mount the core dashboard application
        routes.append(
            Include(
                "/",
                app=create_dashboard_app(),
            )
        )

        # 3. Create the ChildLilya app
        app: ChildLilya = ChildLilya(
            middleware=middlewares,
            routes=routes,
        )

        return app

    def include_in(self, app: Lilya) -> None:
        """
        Mounts the dashboard's internal `ChildLilya` application onto a parent `Lilya` application.

        Args:
            app: The host `Lilya` application instance.
        """
        app.add_child_lilya(self.url_prefix, self.child_app)

    def get_asgi_app(self, with_url_prefix: bool = False) -> ASGIApp:
        """
        Returns the dashboard's internal `object` application to be mounted in any ASGI framework.

        Returns:
            ASGIApp: The application instance.
        """
        if with_url_prefix:
            return cast(
                ASGIApp,
                Lilya(routes=[Include(path=self.url_prefix, app=self.child_app)]),
            )
        return cast(ASGIApp, self.child_app)
