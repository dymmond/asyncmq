# Authentication Backends

Dashboard authentication is pluggable through the `AuthBackend` protocol.

## Auth Flow

```mermaid
sequenceDiagram
    participant U as User Browser
    participant M as AuthGateMiddleware
    participant B as AuthBackend
    participant A as Dashboard App

    U->>M: HTTP request
    M->>B: authenticate(request)
    alt authenticated
        B-->>M: user
        M->>A: forward request
        A-->>U: page response
    else not authenticated
        B-->>M: None
        M-->>U: redirect /login (or HX-Redirect)
    end
```

## JWTAuthBackend

`JWTAuthBackend` validates `Authorization: Bearer <token>` headers with PyJWT.

```python
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.backends.jwt import JWTAuthBackend

backend = JWTAuthBackend(
    secret="change-me-with-strong-secret",
    algorithms=["HS256"],
    audience=None,
    issuer=None,
    user_claim="sub",
    user_name_claim="name",
    leeway=0,
)

admin = AsyncMQAdmin(enable_login=True, backend=backend)
```

Behavior:

- `authenticate()` decodes token and returns a user dict (`id`, `name`, `claims`) or `None`.
- `login()` returns informational HTML (JWT issuance is external to AsyncMQ).
- `logout()` redirects to `/login`.

Install dependency:

```bash
pip install pyjwt
```

### Token Creation Example

```python
import jwt

payload = {"sub": "ops-user", "name": "Ops User"}
token = jwt.encode(payload, "change-me-with-strong-secret", algorithm="HS256")
print(token)
```

Use in request header:

```text
Authorization: Bearer <token>
```

### Hardening Checklist for JWT

1. Use a secret of at least 32 bytes for HS256.
2. Set `audience` and `issuer` if tokens come from a central IdP.
3. Keep token lifetimes short and rotate signing keys.
4. Terminate TLS before any dashboard path.

## SimpleUsernamePasswordBackend

Session-backed auth using a synchronous `verify(username, password)` callback.

```python
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.backends.simple_user import (
    SimpleUsernamePasswordBackend,
)
from asyncmq.contrib.dashboard.admin.protocols import User


def verify(username: str, password: str) -> User | None:
    if username == "admin" and password == "secret":
        return User(id="admin", name="Admin", is_admin=True)
    return None


backend = SimpleUsernamePasswordBackend(verify=verify)
admin = AsyncMQAdmin(enable_login=True, backend=backend)
```

## Mount Prefix and Auth Redirects

When mounting under a prefix, build app links with `with_url_prefix=True` to keep login/logout and sidebar links correct.

```python
from fastapi import FastAPI
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

app = FastAPI()
admin = AsyncMQAdmin(enable_login=True, backend=backend)
app.mount("/ops", admin.get_asgi_app(with_url_prefix=True))
```

## Security Checklist

- Enforce HTTPS.
- Rotate JWT/session secrets.
- Keep auth backend errors opaque to clients.
- Restrict dashboard exposure to trusted operator networks.
- Review `/audit` regularly for sensitive actions.
