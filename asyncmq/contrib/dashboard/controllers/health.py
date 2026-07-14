from __future__ import annotations

from typing import Any

from lilya.controllers import Controller
from lilya.requests import Request
from lilya.responses import JSONResponse

from asyncmq import monkay


class HealthController(Controller):
    async def get(self, request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok", "service": "asyncmq-dashboard"})


class ReadinessController(Controller):
    async def get(self, request: Request) -> JSONResponse:
        backend: Any = monkay.settings.backend
        backend_name = backend.__class__.__name__
        try:
            health_check = getattr(backend, "health_check", None)
            if callable(health_check):
                await health_check()
        except Exception as exc:
            return JSONResponse(
                {
                    "status": "error",
                    "backend": backend_name,
                    "error": f"{exc.__class__.__name__}: {exc}",
                },
                status_code=503,
            )

        return JSONResponse(
            {
                "status": "ok",
                "backend": backend_name,
            }
        )
