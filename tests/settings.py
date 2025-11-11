from dataclasses import dataclass
from typing import Sequence

from lilya.conf.global_settings import Settings as LilyaSettings
from lilya.middleware import DefineMiddleware, Middleware
from lilya.middleware.sessions import SessionMiddleware

from asyncmq.conf.global_settings import Settings
from asyncmq.core.utils.dashboard import DashboardConfig

test_scanner_interval = 0.01


class LilyaDashboardSettings(LilyaSettings):
    @property
    def middleware(self) -> Sequence[DefineMiddleware]:
        return [
            Middleware(SessionMiddleware, secret_key="secret"),
        ]


@dataclass
class DashConfig(DashboardConfig):
    title: str = "AsyncMQ"
    header_text: str = "AsyncMQ Dashboard"
    favicon: str = "/static/favicon.png"
    sidebar_bg_colour: str = "#111827"
    dashboard_url_prefix: str = "/asyncmq"


class TestSettings(Settings):
    debug: bool = True
    asyncmq_postgres_backend_url: str = "postgresql://postgres:postgres@localhost:5432/postgres"
    stalled_check_interval: float = test_scanner_interval
    stalled_threshold: float = test_scanner_interval
    scan_interval: float = 0.1
    asyncmq_postgres_pool_options: dict[str, int] = {"min_size": 1, "max_size": 4}

    @property
    def dashboard_config(self) -> DashboardConfig | None:
        return DashConfig()
