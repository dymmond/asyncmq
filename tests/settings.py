from dataclasses import dataclass

from asyncmq.conf.global_settings import Settings

test_scanner_interval = 0.01

@dataclass
class TestSettings(Settings):
    debug: bool = True
    asyncmq_postgres_backend_url: str = "postgresql://postgres:postgres@localhost:5432/postgres"
    stalled_check_interval: float = test_scanner_interval
    stalled_threshold: float = test_scanner_interval

    asyncmq_postgres_pool_options = {
        "min_size": 10,
        "max_size" : 200,
    }
