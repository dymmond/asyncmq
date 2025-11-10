"""
This file is only used for development purposes.
Please do not use this anywhere in your code base
"""

from lilya.apps import Lilya
from lilya.routing import Include

from asyncmq.contrib.dashboard.admin import (
    AsyncMQAdmin,
    SimpleUsernamePasswordBackend,
    User,
)


def verify(u: str, p: str) -> User | None:
    if u == "admin" and p == "secret":
        return User(id="admin", name="Admin")
    return None


asynmq_admin = AsyncMQAdmin(enable_login=True, backend=SimpleUsernamePasswordBackend(verify))  # type: ignore
app = Lilya(routes=[Include(asynmq_admin.url_prefix, app=asynmq_admin.get_asgi_app())])
asynmq_admin.include_in(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("run:app", host="0.0.0.0", port=8000, reload=True)
