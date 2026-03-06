# Dashboard Authentication Backends

AsyncMQ dashboard authentication is pluggable via `AuthBackend`.

## JWTAuthBackend

`JWTAuthBackend` validates `Authorization: Bearer <token>` headers with PyJWT.

```python
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.backends.jwt import JWTAuthBackend

backend = JWTAuthBackend(
    secret="change-me",
    algorithms=["HS256"],
    audience=None,
    issuer=None,
    user_claim="sub",
    user_name_claim="name",
)

admin = AsyncMQAdmin(enable_login=True, backend=backend)
```

Behavior:
- `authenticate()` decodes JWT and returns user payload or `None`.
- `login()` returns informational HTML (JWT is stateless).
- `logout()` redirects to `/login`.

Install requirement:

```bash
pip install pyjwt
```

## SimpleUsernamePasswordBackend

`SimpleUsernamePasswordBackend` uses server-side sessions and a synchronous `verify(username, password)` callback.

```python
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.backends.simple_user import (
    SimpleUsernamePasswordBackend,
)
from asyncmq.contrib.dashboard.admin.protocols import User


def verify(username: str, password: str) -> User | None:
    if username == "admin" and password == "secret":
        return User(id="admin", name="Admin")
    return None


backend = SimpleUsernamePasswordBackend(verify=verify)
admin = AsyncMQAdmin(enable_login=True, backend=backend)
```

## Security Recommendations

- Always use HTTPS in production.
- Rotate JWT secrets and session secrets regularly.
- Keep dashboard mounted behind network and identity controls when possible.
