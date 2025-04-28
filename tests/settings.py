from dataclasses import dataclass

from asyncmq.conf.global_settings import Settings


@dataclass
class TestSettings(Settings):
    debug: bool = True
