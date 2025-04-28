from dataclasses import dataclass

from asyncmq.conf.global_settings import Settings


@dataclass
class TestSettings(Settings):
    debug: bool = True
    asyncmq_postgres_backend_url: str = "postgresql://postgres:postgres@localhost:5432/postgres"
