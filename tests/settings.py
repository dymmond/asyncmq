from dataclasses import dataclass, field

from asyncmq.conf.global_settings import Settings

test_scanner_interval = 0.01


@dataclass
class TestSettings(Settings):
    debug: bool = True
    asyncmq_postgres_backend_url: str = "postgresql://postgres:postgres@localhost:5432/postgres"
    stalled_check_interval: float = test_scanner_interval
    stalled_threshold: float = test_scanner_interval

    asyncmq_postgres_pool_options: dict | None = field(
        default_factory=lambda: {
            "min_size": 1,
            "max_size": 4,
        }
    )
