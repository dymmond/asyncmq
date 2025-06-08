from dataclasses import dataclass, field

from asyncmq.conf.global_settings import Settings

test_scanner_interval = 0.01


@dataclass
class Settings(Settings):
    debug: bool = True
    # backend: BaseBackend = RedisBackend(redis_url="redis://:bigAnimalFarm34Rdb@server.nivabit.com:6379/2")
    stalled_check_interval: float = test_scanner_interval
    stalled_threshold: float = test_scanner_interval
    scan_interval: float = 0.1

    asyncmq_postgres_pool_options: dict | None = field(
        default_factory=lambda: {
            "min_size": 1,
            "max_size": 4,
        }
    )
