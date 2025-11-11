from __future__ import annotations

from typing import Any, cast

try:
    import jwt
except ImportError:
    raise ValueError("PyJWT must be installed to use this backend.") from None

from lilya.requests import Request
from lilya.responses import HTMLResponse, RedirectResponse, Response

from asyncmq.contrib.dashboard.admin.protocols import AuthBackend


class JWTAuthBackend(AuthBackend):
    """
    Stateless JWT authentication backend via the `Authorization: Bearer <token>` header.

    This backend decodes and validates a JWT using the provided secret, algorithms,
    and claims validation (audience, issuer, expiration). It extracts user identity
    from the token claims without relying on server-side session state.

    Typical usage:
        backend = JWTAuthBackend(
            secret="supersecret",
            algorithms=["HS256"],
            audience=None,
            issuer=None,
            header="Authorization",
            scheme="Bearer",
            user_claim="sub",
            user_name_claim="name",
            leeway=0,
            verify_options=None,
        )
    """

    def __init__(
        self,
        *,
        secret: str | None = None,
        algorithms: list[str] | tuple[str, ...] = ("HS256",),
        audience: str | None = None,
        issuer: str | None = None,
        header: str = "Authorization",
        scheme: str = "Bearer",
        user_claim: str = "sub",
        user_name_claim: str = "name",
        leeway: int | float = 0,
        verify_options: dict[str, bool] | None = None,
    ) -> None:
        """
        Initializes the JWT authentication backend with configuration details.

        Args:
            secret: The secret key used for decoding and signature verification. Required for HS* algorithms.
            algorithms: List or tuple of allowed JWT algorithms (e.g., ["HS256", "RS256"]).
            audience: The expected audience claim (`aud`).
            issuer: The expected issuer claim (`iss`).
            header: The HTTP header name containing the token (default: "Authorization").
            scheme: The expected scheme prefix (e.g., "Bearer").
            user_claim: The claim key used to extract the user's unique ID (default: "sub").
            user_name_claim: The claim key used to extract the user's display name (default: "name").
            leeway: Number of seconds of clock skew to allow during time validation.
            verify_options: Dictionary of options passed directly to `jwt.decode` for validation control
                            (e.g., `{"verify_exp": True}`). Defaults to `{"verify_exp": True}`.
        """
        self.secret: str | None = secret
        self.algorithms: list[str] = list(algorithms)
        self.audience: str | None = audience
        self.issuer: str | None = issuer
        self.header: str = header
        self.scheme: str = scheme
        self.user_claim: str = user_claim
        self.user_name_claim: str = user_name_claim
        self.leeway: int | float = leeway
        self.verify_options: dict[str, bool] = verify_options or {"verify_exp": True}

    async def authenticate(self, request: Request) -> dict[str, Any] | None:
        """
        Extracts the token from the configured header, decodes and validates it,
        and returns user data if successful.

        Args:
            request: The incoming request object.

        Returns:
            A dictionary containing user 'id', 'name', and the raw 'claims' payload,
            or `None` if the token is missing, malformed, or invalid.
        """
        auth: str | None = request.headers.get(self.header)

        if not auth or " " not in auth:
            return None

        # Check for scheme prefix (e.g., "Bearer")
        prefix: str
        token: str
        prefix, token = auth.split(" ", 1)
        if prefix.strip().lower() != self.scheme.lower():
            return None

        try:
            # Decode and validate the token signature and claims
            payload: dict[str, Any] = jwt.decode(
                token,
                self.secret,
                algorithms=self.algorithms,
                audience=self.audience,
                issuer=self.issuer,
                leeway=self.leeway,
                options=self.verify_options,
            )
        except Exception:
            # Any decoding/validation error (signature, expiration, claims) => unauthenticated
            return None

        # Extract required user claims
        user_id: str | None = cast(str | None, payload.get(self.user_claim))
        if not user_id:
            return None

        user_name: str = cast(str | None, payload.get(self.user_name_claim)) or user_id

        # Expose claims for templates/logic if needed
        return {"id": user_id, "name": user_name, "claims": payload}

    async def login(self, request: Request) -> Response:
        """
        Handles the logic for the `/login` path. Since JWT is stateless and token-based,
        this endpoint provides an informational message rather than a form.

        Returns:
            An HTML response with guidance on providing a valid JWT header.
        """
        return HTMLResponse("This deployment expects a valid 'Authorization: Bearer 'ey.....;' token header.")

    async def logout(self, request: Request) -> Response:
        """
        Handles the logic for the `/logout` path. Since this is a stateless backend,
        nothing is cleared server-side.

        Returns:
            A redirect response, typically back to the login path.
        """
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[Any]:
        """
        Optional method to return extra routing definitions.

        Returns:
            An empty list, as stateless JWT authentication does not require server-side routes (forms, etc.).
        """
        return []
