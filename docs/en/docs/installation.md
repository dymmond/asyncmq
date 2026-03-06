# Installation

## Python Version

AsyncMQ requires Python `>=3.10`.

## Install Options

Base install (includes Redis dependency):

```bash
pip install asyncmq
```

Optional backend extras:

```bash
pip install "asyncmq[postgres]"   # asyncpg
pip install "asyncmq[mongo]"      # motor
pip install "asyncmq[aio-pika]"   # RabbitMQ backend
pip install "asyncmq[all]"        # all optional backends
```

## Configure Settings Module

AsyncMQ loads settings from `ASYNCMQ_SETTINGS_MODULE`.

```bash
export ASYNCMQ_SETTINGS_MODULE=myapp.settings.AppSettings
```

Example settings:

```python
from asyncmq.backends.redis import RedisBackend
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    backend = RedisBackend("redis://localhost:6379/0")
    worker_concurrency = 4
    scan_interval = 1.0
```

## Verify Installation

```bash
asyncmq --help
asyncmq info version
asyncmq info backend
```

Continue with [Quickstart](features/quickstart.md).
