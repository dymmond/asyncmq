from typing import Any, cast

from lilya.apps import Lilya
from lilya.types import Receive, Scope, Send

from asyncmq import monkay
from asyncmq.contrib.dashboard.engine import templates  # noqa


class CompatibleURL:
    """URL-like object that provides make_absolute_url method for Lilya compatibility"""

    def __init__(self, path: str):
        self.path = path

    def make_absolute_url(self, base_url: str | None = None) -> str:
        return self.path

    def __str__(self) -> str:
        return self.path


class AsgiCompatibleRouter:
    """Router wrapper that provides path_for method for ASGI compatibility"""

    def __init__(self, original_router: Any, mount_path: str = "") -> None:
        self.original_router = original_router
        self.mount_path = mount_path

    def url_path_for(self, name: str, **path_params: Any) -> CompatibleURL:
        # Use configured dashboard prefix and the mount path for correctness under FastAPI mounts
        configured_prefix = monkay.settings.dashboard_config.dashboard_url_prefix or ""
        base = f"{self.mount_path}{configured_prefix}"
        if name == "statics":
            path = path_params.get("path", "")
            return CompatibleURL(f"{base}/static{path}")
        return CompatibleURL(f"{base}")

    def __getattr__(self, name: str) -> Any:
        return getattr(self.original_router, name)


class UnifiedDashboard:
    """Dashboard that works with both Lilya and FastAPI environments"""

    def __init__(self, lilya_app: Lilya) -> None:
        self.lilya_app = lilya_app
        # Expose the router for Lilya compatibility
        self.router = lilya_app.router

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] == "http" and scope.get("router") and not hasattr(scope.get("router"), "path_for"):
            # We're in a FastAPI/Starlette context, apply compatibility
            scope = dict(scope)
            mount_path = cast(str, scope.get("root_path", "").rstrip("/"))
            scope["router"] = AsgiCompatibleRouter(scope["router"], mount_path)

        await self.lilya_app(scope, receive, send)

    def __getattr__(self, name: str) -> Any:
        """Delegate attributes to the underlying Lilya app for compatibility"""
        return getattr(self.lilya_app, name)
