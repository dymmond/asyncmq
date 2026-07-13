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

## Structured JSON Logs

Set `structured_logging=True` to make the built-in logging configuration emit
one JSON object per log line:

```python
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    structured_logging = True
```

JSON log records include `timestamp`, `level`, `logger`, `module`, and
`message`. Extra fields passed through the standard logging `extra={...}`
mechanism are preserved.

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
