from dataclasses import dataclass

from asyncmq.conf.global_settings import Settings as BaseSettings
from asyncmq.core.utils.dashboard import DashboardConfig


@dataclass
class DevConfig(DashboardConfig):
    secret_key: str = "development"
    session_cookie: str = "dev_asyncmq_admin"


class Settings(BaseSettings):
    @property
    def dashboard_config(self) -> DevConfig:
        return DevConfig()
