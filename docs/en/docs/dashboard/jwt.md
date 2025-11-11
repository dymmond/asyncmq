# JWT Authentication for the AsyncMQ Admin

The AsyncMQ Admin can be protected using a JSON Web Token (JWT) carried in the `Authorization` header.

This page shows how to enable JWT auth, how tokens are validated, and how to build your own custom backend that
checks a user in a database.

!!! info "Good to know"
    - Admin wrapper class: `asyncmq.contrib.dashboard.admin.AsyncMQAdmin`
    - Auth protocol: `asyncmq.contrib.dashboard.admin.protocols.AuthBackend`
    - Gate middleware: `asyncmq.contrib.dashboard.admin.middleware.AuthGateMiddleware`
    - Default mount prefix (configurable): `/asyncmq`


This is a special backend that also requires you to install additional packages.

## Quick start

### Install a JWT library

You can use any JWT library you like. The examples below use **PyJWT**:

```bash
pip install "pyjwt>=2.9"
```

### Implement a `JWTAuthBackend`

Create a backend that:

- Extracts the bearer token from `Authorization: Bearer <token>`,
- Verifies signature/claims,
- Returns a user dict (or `None`) from `authenticate`,
- Defines a `login` view (can be a helpful page for developers),
- Defines a `logout` view (stateless JWT → usually just redirect).

```python
from __future__ import annotations

import typing as t
import jwt
from lilya.requests import Request
from lilya.responses import HTMLResponse, RedirectResponse, Response
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend


class JWTAuthBackend(AuthBackend):
    """
    Minimal JWT auth for the AsyncMQ Admin.

    - Reads token from `Authorization: Bearer <token>`
    - Verifies signature/claims with a shared secret (HS256 here, but RS256 works too)
    - Returns a user dict on success, otherwise None
    """

    def __init__(
        self,
        secret: str,
        algorithm: str = "HS256",
        audience: str | None = None,
        issuer: str | None = None,
        header_name: str = "Authorization",
        cookie_name: str | None = None,   # optionally support a token in a cookie
    ) -> None:
        self.secret = secret
        self.algorithm = algorithm
        self.audience = audience
        self.issuer = issuer
        self.header_name = header_name
        self.cookie_name = cookie_name

    def _extract_token(self, request: Request) -> str | None:
        # Priority: header, then cookie (if enabled)
        header = request.headers.get(self.header_name)
        if header and header.lower().startswith("bearer "):
            return header.split(" ", 1)[1].strip()

        if self.cookie_name:
            cookie_val = request.cookies.get(self.cookie_name)
            if cookie_val and cookie_val.lower().startswith("bearer "):
                return cookie_val.split(" ", 1)[1].strip()
        return None

    async def authenticate(self, request: Request) -> dict | None:
        token = self._extract_token(request)
        if not token:
            return None

        options: dict[str, t.Any] = {"verify_signature": True, "verify_exp": True}
        verify_args: dict[str, t.Any] = {"key": self.secret, "algorithms": [self.algorithm]}
        if self.audience:
            verify_args["audience"] = self.audience
        if self.issuer:
            verify_args["issuer"] = self.issuer

        try:
            claims = jwt.decode(token, **verify_args, options=options)
        except jwt.PyJWTError:
            # invalid / expired / wrong issuer / wrong audience / bad signature
            return None

        # Expect minimal identity claims; adapt to your token shape.
        sub = claims.get("sub")
        name = claims.get("name") or sub
        if not sub:
            return None

        # Return a plain dict; AsyncMQ only needs truthy "someone" to pass the gate.
        # You can attach roles/permissions here if you want to authorize specific actions later.
        return {"id": sub, "name": name, "claims": claims}

    async def login(self, _: Request) -> Response:
        # This can be an informative page or a 405. Keep simple for devs:
        return HTMLResponse(
            "This deployment expects a valid <code>Authorization: Bearer token header."
        )

    async def logout(self, _: Request) -> Response:
        # Stateless JWT has nothing to clear; redirect back to login.
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[t.Any]:
        # Provide no extra routes here; the admin mounts /login and /logout automatically.
        return []
```

### Mount the admin with the JWT backend

```python
import os
from lilya.apps import Lilya
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

# from the code above
from yourproject.auth_backends import JWTAuthBackend

app = Lilya()

admin = AsyncMQAdmin(
    enable_login=True,
    backend=JWTAuthBackend(
        secret=os.environ["ASYNCMQ_JWT_SECRET"],
        algorithm="HS256",
        audience="asyncmq-admin",         # optional
        issuer="https://issuer.example",  # optional
    ),
    include_session=True,  # sessions are fine; JWT remains stateless for identity
    include_cors=False,
)
admin.include_in(app)
```

Visit: `http://localhost:8000/asyncmq/` with a valid `Authorization: Bearer <jwt>` header.

## Issuing a token (example)

With **PyJWT**:

```python
import os, time, jwt

now = int(time.time())
payload = {
    "sub": "user-1234",
    "name": "Alice",
    "aud": "asyncmq-admin",           # must match backend if set
    "iss": "https://issuer.example",  # must match backend if set
    "iat": now,
    "exp": now + 3600,                  # 1 hour
    "roles": ["admin"],               # custom claims ok
}

token = jwt.encode(
    payload,
    os.environ["ASYNCMQ_JWT_SECRET"],
    algorithm="HS256",
)

print("Authorization: Bearer", token)
```

## Behavior overview

- **Unauthenticated request** → `303 See Other` to `/asyncmq/login?next=/asyncmq/...`
- **HTMX partial requests** → `401` with `HX-Redirect: /asyncmq/login?next=...`
- **Invalid/expired token** → treated as unauthenticated (same redirects as above)
- **Logout** (`/asyncmq/logout`) → stateless (no server session to clear), redirects to `/login`

!!! tip
    If you rely on *session-backed* state in your app, you can optionally store a small cache on `request.session`
    after a successful `authenticate`. For pure JWT flows, it's simpler to remain stateless.

---

## Configuration & settings

Most deployments only need the backend parameters. For completeness:

- `AsyncMQAdmin(url_prefix="/asyncmq", include_session=True, include_cors=True, login_path="/login", allowlist=("/login", "/logout", "/static", "/assets"))`
- The dashboard's visual config (title, colors, prefix, favicon) comes from `settings.dashboard_config` (see the main dashboard docs).
- If your reverse proxy injects headers, consider a header-only backend (see [Proxy/SSO header backend](#proxysso-header-backend-no-jwt)).

## Security notes

- Prefer **RS256/ES256** with a public key in the admin and a private key on your IdP/signing service, if available.
- Keep `exp` short. Rotate secrets/keys regularly.
- Validate **issuer** and **audience** when you control both ends.
- Avoid putting sensitive PII in token claims. Use a user ID and fetch details server-side if needed.

## Real-world: custom auth backend with DB user validation

You may want to accept a JWT **and** verify the user exists (and is active/admin) in your database.

Below is an example using **SQLAlchemy** (async) and **PyJWT**. Adapt to your stack.

```python
from __future__ import annotations

import typing as t
import jwt
from sqlalchemy.ext.asyncio import AsyncSession
from lilya.requests import Request
from lilya.responses import HTMLResponse, RedirectResponse, Response
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend

# your SQLAlchemy model with fields: id, is_active, is_admin
from myapp.db import get_async_session
from myapp.models import User


class JWTWithDBAuthBackend(AuthBackend):
    """
    - Verifies JWT
    - Looks up user in DB by `sub`
    - Requires `is_active=True` and (optionally) `is_admin=True`
    """

    def __init__(
        self,
        secret: str,
        algorithm: str = "HS256",
        audience: str | None = None,
        issuer: str | None = None,
        require_admin: bool = True,
    ) -> None:
        self.secret = secret
        self.algorithm = algorithm
        self.audience = audience
        self.issuer = issuer
        self.require_admin = require_admin

    async def authenticate(self, request: Request) -> dict | None:
        header = request.headers.get("Authorization")
        if not header or not header.lower().startswith("bearer "):
            return None

        token = header.split(" ", 1)[1].strip()
        try:
            claims = jwt.decode(
                token,
                key=self.secret,
                algorithms=[self.algorithm],
                audience=self.audience,
                issuer=self.issuer,
                options={"verify_signature": True, "verify_exp": True},
            )
        except jwt.PyJWTError:
            return None

        user_id = claims.get("sub")
        if not user_id:
            return None

        # DB check
        async with get_async_session() as session:  # type: AsyncSession
            user = await self._get_user(session, user_id)
            if not user or not user.is_active:
                return None
            if self.require_admin and not getattr(user, "is_admin", False):
                return None

        return {
            "id": str(user.id),
            "name": getattr(user, "display_name", None) or getattr(user, "email", str(user.id)),
            "claims": claims,
            "is_admin": getattr(user, "is_admin", False),
        }

    async def _get_user(self, session: AsyncSession, user_id: str) -> User | None:
        # Example SQLAlchemy 2.x style; adjust to your schema/query helpers.
        result = await session.execute(
            User.__table__.select().where(User.id == user_id)
        )
        row = result.first()
        return User(**row._mapping) if row else None

    async def login(self, _: Request) -> Response:
        return HTMLResponse(
            "Present a valid Bearer token for an existing, active user (admin required)."
        )

    async def logout(self, _: Request) -> Response:
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[t.Any]:
        return []
```

### Mounting

```python
import os
from lilya.apps import Lilya
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from yourproject.auth_backends import JWTWithDBAuthBackend

app = Lilya()
admin = AsyncMQAdmin(
    enable_login=True,
    backend=JWTWithDBAuthBackend(
        secret=os.environ["ASYNCMQ_JWT_SECRET"],
        algorithm="HS256",
        audience="asyncmq-admin",
        issuer="https://issuer.example",
        require_admin=True,  # only admins can access the admin UI
    ),
    include_session=True,
    include_cors=False,
)
admin.include_in(app)
```

!!! Note "Why DB validation?"
    - Disable suspended users immediately.
    - Enforce admin-only access.
    - Map roles/permissions from your own tables rather than token claims.

## Proxy/SSO header backend (no JWT)

If your reverse proxy / gateway (e.g. OAuth2 proxy) injects an authenticated header, use a tiny header backend:

```python
from __future__ import annotations
import typing as t
from lilya.requests import Request
from lilya.responses import HTMLResponse, RedirectResponse, Response
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend

class ProxyHeaderBackend(AuthBackend):
    def __init__(self, header: str = "X-Authenticated-User"):
        self.header = header

    async def authenticate(self, request: Request) -> dict | None:
        sub = request.headers.get(self.header)
        return {"id": sub, "name": sub} if sub else None

    async def login(self, _: Request) -> Response:
        return HTMLResponse("This deployment expects an authenticated proxy header.")

    async def logout(self, _: Request) -> Response:
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[t.Any]:
        return []
```

## Testing (pytest + Lilya TestClient)

When testing redirects, **disable following redirects** or you'll assert against the final 200 page:

```python
def test_rejects_bad_token(client):
    res = client.get(
        "/asyncmq/",
        headers={"Authorization": "Bearer bad"},
        follow_redirects=False,
    )
    assert res.status_code == 303
    assert res.headers["location"].startswith("/asyncmq/login")
```

For positive paths:

```python
def test_allows_valid_token(client, token):
    res = client.get(
        "/asyncmq/",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert res.status_code == 200
    assert "Dashboard" in res.text
```

## Troubleshooting

- **Got 303 to /login?** Your backend returned `None` from `authenticate`. Check:
    - No token? Wrong header? (`Authorization: Bearer ...`)
    - Signature/algorithm mismatch?
    - `aud`/`iss` mismatch?
    - `exp` in the past?
    - User not found/inactive/admin?
- **Local dev:** Set a static secret in `.env` for simplicity and rotate/change in staging/production.


## API reference (relevant pieces)

```python
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

# implement this
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend
from asyncmq.contrib.dashboard.admin.middleware import AuthGateMiddleware
```

That's it, drop in the JWT backend to guard the AsyncMQ Admin, or roll your own that validates users in your DB.

## Final notes

These serve as guidelines if you want to implement JWT. AsyncMQ provides one but most likely you will need to customise
to your own needs.
