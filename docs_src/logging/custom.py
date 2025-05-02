import logging
from logging import FileHandler, Formatter
from typing import Any

from asyncmq.logging import LoggingConfig


class MyLoggingConfig(LoggingConfig):
    def __init__(self, level: str, filename: str):
        super().__init__(level=level)
        self.filename = filename

    def configure(self) -> None:
        import logging
        lvl = getattr(logging, self.level)
        fmt = "%(asctime)s %(levelname)s %(message)s"
        handler = FileHandler(self.filename)
        handler.setFormatter(Formatter(fmt))
        root = logging.getLogger()
        root.addHandler(handler)
        root.setLevel(lvl)

    def get_logger(self) -> Any:
        return logging.getLogger("myapp")
