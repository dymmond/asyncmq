from __future__ import annotations

import os

from asyncmq.conf.global_settings import Settings
from asyncmq.core.utils.dashboard import DashboardConfig


class NginxProofSettings(Settings):
    """Settings used by the Nginx dashboard browser proof app."""

    @property
    def dashboard_config(self) -> DashboardConfig:
        """Return a dashboard config whose prefix matches the proof scenario."""
        return DashboardConfig(
            secret_key="nginx-proof-secret",
            dashboard_url_prefix=os.environ.get("ASYNCMQ_DASHBOARD_PREFIX", "/asyncmq"),
        )
