from dataclasses import dataclass


@dataclass
class DashboardConfig:
    title: str = "Dashboard"
    header_title: str = "AsyncMQ"
    description: str = "A simple dashboard for monitoring AsyncMQ jobs."
    favicon: str = "https://raw.githubusercontent.com/dymmond/asyncmq/refs/heads/main/docs/statics/favicon.ico"
    dashboard_url_prefix: str = "/admin"
    sidebar_bg_colour: str = "#CBDC38"
