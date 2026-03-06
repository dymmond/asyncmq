# Logging

AsyncMQ exposes a pluggable logging configuration interface in `asyncmq.logging`.

## Default Behavior

- `logger` is a proxy (`LoggerProxy`).
- On first logger access, `setup_logging()` is invoked lazily.
- Default configuration is `StandardLoggingConfig` (console handler, standard format).

## Configure Log Level

```python
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    logging_level = "DEBUG"
```

## Custom Logging Backend

Implement `LoggingConfig`:

```python
from asyncmq.logging import LoggingConfig


class MyLoggingConfig(LoggingConfig):
    def configure(self) -> None:
        ...

    def get_logger(self):
        ...
```

Then override `settings.logging_config` to return your custom config instance.

## Notes

- `setup_logging(logging_config=...)` validates config type.
- `setup_logging()` binds logger globally for AsyncMQ components.
