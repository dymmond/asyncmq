from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import Any

from lilya.requests import Request
from lilya.responses import PlainText, RedirectResponse, Response
from lilya.types import ASGIApp, Receive, Scope, Send

AuthenticateCallable = Callable[[Request], Awaitable[Any | None]]
TRUTHY_VALUES = {"1", "true", "yes", "on"}
DEFAULT_SCHEME_PORTS = {"http": "80", "https": "443"}


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in TRUTHY_VALUES
    return False


def _coerce_roles(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value}
    if isinstance(value, Mapping):
        return set()
    if isinstance(value, Iterable):
        return {str(role) for role in value if role}
    return {str(value)}


def _user_roles(user: Any) -> set[str]:
    roles: set[str] = set()
    if isinstance(user, Mapping):
        roles.update(_coerce_roles(user.get("roles")))
        roles.update(_coerce_roles(user.get("role")))
        claims = user.get("claims")
        if isinstance(claims, Mapping):
            roles.update(_coerce_roles(claims.get("roles")))
            roles.update(_coerce_roles(claims.get("role")))
        return roles

    roles.update(_coerce_roles(getattr(user, "roles", None)))
    extra = getattr(user, "extra", None)
    if isinstance(extra, Mapping):
        roles.update(_coerce_roles(extra.get("roles")))
        roles.update(_coerce_roles(extra.get("role")))
    return roles


def _user_is_admin(user: Any) -> bool:
    roles = _user_roles(user)
    if "admin" in roles or "asyncmq:admin" in roles:
        return True

    if isinstance(user, Mapping):
        admin_value = user.get("is_admin")
        claims = user.get("claims")
        if admin_value is None and isinstance(claims, Mapping):
            admin_value = claims.get("is_admin", claims.get("admin"))
        return _truthy(admin_value)

    return _truthy(getattr(user, "is_admin", False))


def _first_header_value(value: str | None) -> str | None:
    """Return the first comma-separated forwarded-header value, if present."""
    if not value:
        return None
    return value.split(",", 1)[0].strip() or None


def _client_peer(scope: Scope) -> str:
    """Return the ASGI client peer address used for trusted-proxy checks."""
    client = scope.get("client")
    if isinstance(client, (tuple, list)) and client:
        return str(client[0])
    return "unix"


def _host_has_port(host: str) -> bool:
    """Return whether a host string already carries an explicit port."""
    if host.startswith("["):
        return "]:" in host
    return ":" in host


def _origin_from_parts(scheme: str, host: str, port: str | None = None) -> str:
    """Build a normalized origin string from request or trusted proxy parts."""
    scheme = scheme.split(",", 1)[0].strip()
    host = host.split(",", 1)[0].strip()
    port = _first_header_value(port)
    if port and not _host_has_port(host) and port != DEFAULT_SCHEME_PORTS.get(scheme):
        host = f"{host}:{port}"
    return f"{scheme}://{host}"


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
        enforce_same_origin: bool = True,
        require_admin: bool = True,
        required_roles: Iterable[str] = (),
        trusted_proxies: Iterable[str] = (),
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
            enforce_same_origin: If True, unsafe authenticated requests must come
                       from the same origin as the dashboard host.
            require_admin: If True, authenticated users must be admins.
            required_roles: Optional role allowlist. When provided, the user
                       must have at least one required role.
            trusted_proxies: Client peer addresses whose forwarded host/proto
                       headers may be trusted for same-origin checks.
        """
        self.app: ASGIApp = app
        self.authenticate: AuthenticateCallable = authenticate
        self.login_path: str = login_path
        self.allowlist: tuple[str, ...] = tuple(allowlist)
        self.enforce_same_origin = enforce_same_origin
        self.require_admin = require_admin
        self.required_roles = frozenset(str(role) for role in required_roles)
        self.trusted_proxies = frozenset(str(proxy) for proxy in trusted_proxies)

    def _trusts_forwarded_headers(self, request: Request) -> bool:
        """Return whether this request may use reverse-proxy origin headers."""
        if not self.trusted_proxies:
            return False
        return "*" in self.trusted_proxies or _client_peer(request.scope) in self.trusted_proxies

    def _is_same_origin(self, request: Request) -> bool:
        """Return whether an unsafe request came from the dashboard origin."""
        origin = request.headers.get("origin")
        if not origin:
            return False

        forwarded_trusted = self._trusts_forwarded_headers(request)
        host = (
            _first_header_value(request.headers.get("x-forwarded-host"))
            if forwarded_trusted
            else None
        ) or request.headers.get("host")
        if not host:
            return False

        scheme = (
            _first_header_value(request.headers.get("x-forwarded-proto"))
            if forwarded_trusted
            else None
        ) or request.scope.get("scheme", "http")
        port = request.headers.get("x-forwarded-port") if forwarded_trusted else None
        return origin == _origin_from_parts(str(scheme), host, port)

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
            if self.require_admin and not _user_is_admin(user):
                response = PlainText("Dashboard user is not authorized", status_code=403)
                await response(scope, receive, send)
                return
            roles = _user_roles(user)
            if self.required_roles and roles.isdisjoint(self.required_roles):
                response = PlainText("Dashboard user is not authorized", status_code=403)
                await response(scope, receive, send)
                return
            if self.enforce_same_origin and request.method in {"POST", "PUT", "PATCH", "DELETE"}:
                if not self._is_same_origin(request):
                    response = PlainText("Cross-origin dashboard request rejected", status_code=403)
                    await response(scope, receive, send)
                    return
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
