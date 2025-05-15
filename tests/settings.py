from dataclasses import dataclass, field
from typing import Sequence

from lilya.conf.global_settings import Settings as LilyaSettings
from lilya.middleware import DefineMiddleware, Middleware
from lilya.middleware.sessions import SessionMiddleware

from asyncmq.conf.global_settings import Settings

test_scanner_interval = 0.01


@dataclass
class LilyaDashboardSettings(LilyaSettings):
    @property
    def middleware(self) -> Sequence[DefineMiddleware]:
        return [
            Middleware(SessionMiddleware, secret_key="secret"),
        ]


@dataclass
class TestSettings(Settings):
    debug: bool = True
    asyncmq_postgres_backend_url: str = "postgresql://postgres:postgres@localhost:5432/postgres"
    stalled_check_interval: float = test_scanner_interval
    stalled_threshold: float = test_scanner_interval
    scan_interval: float = 0.1

    asyncmq_postgres_pool_options: dict | None = field(
        default_factory=lambda: {
            "min_size": 1,
            "max_size": 4,
        }
    )
