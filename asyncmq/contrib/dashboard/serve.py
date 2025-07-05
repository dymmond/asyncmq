import secrets

from lilya.apps import Lilya
from lilya.middleware.base import DefineMiddleware
from lilya.middleware.cors import CORSMiddleware
from lilya.middleware.sessions import SessionMiddleware
from lilya.routing import Include

from asyncmq.contrib.dashboard.application import dashboard

dash_app = Lilya(
    routes=[Include(path="/", app=dashboard)],
    middleware=[
        DefineMiddleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
            allow_credentials=True,
        ),
        DefineMiddleware(
            SessionMiddleware,
            secret_key=secrets.token_hex(32),
        ),
    ],
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("serve:dash_app", host="0.0.0.0", port=8000, reload=True)
