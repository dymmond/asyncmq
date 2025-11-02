from typing import Any

# Logging

AsyncMQ's logging system provides a **consistent**, **configurable**, and **extensible** way to capture insights from
every component, from task producers to workers and schedulers.

This guide dives deep into the **`LoggingConfig`** protocol, the built‑in **`StandardLoggingConfig`**,
and how to customize logging to suit your needs.

---

## 1. What Is the `LoggingConfig` Protocol?

Located in `asyncmq.logging`, `LoggingConfig` defines the interface for configuring and initializing logging in AsyncMQ.

Any implementation must:

* **Expose a `level` attribute** (`str`, e.g., "INFO", "DEBUG").
* **Implement a `configure()` method** that applies the configuration to Python's `logging` module and a `get_logger()`
that returns the instance of the logging being used.

This abstraction lets AsyncMQ treat logging setup as a pluggable component.

---

## 2. The Built‑in `StandardLoggingConfig`

Provided in `asyncmq.core.utils.logging`, `StandardLoggingConfig` is the default implementation:

```python
{!> ../../../docs_src/logging/logging.py !}
```

### Why It Matters

* **Uniform Output**: All modules log with the same format and level.
* **Quick Setup**: One call to `configure()` and `get_logger()` and you're done.
* **Extensible**: Build your own class if you need JSON, file logging, or third‑party handlers.

---

## 3. How AsyncMQ Uses It

1. **Settings Integration**
   In `asyncmq.conf.global_settings.Settings`:

   ```python
   @property
   def logging_config(self) -> LoggingConfig:
       from asyncmq.core.utils.logging import StandardLoggingConfig
       return StandardLoggingConfig(level=self.logging_level)
   ```
2. **CLI & Runner Initialization**
   At startup, AsyncMQ CLI and workers do:

   ```python
   config = settings.logging_config
   if config:
       config.configure()
   ```

   This ensures **before** any commands or job processing runs, the logging framework is fully configured.

---

## 4. Customizing Logging

### 4.1. Override `logging_level`

Simply set a different level in your `Settings` subclass:

```python
class Settings(BaseSettings):
    logging_level = "DEBUG"
```

### 4.2. Provide a Custom `LoggingConfig`

If you need advanced behavior (e.g., JSON output, file rotation), implement `LoggingConfig`:

```python
{!> ../../../docs_src/logging/custom.py !}
```

Then override the `logging_config` property in your settings:

```python
from asyncmq.conf.global_settings import Settings as BaseSettings

class Settings(BaseSettings):
    @property
    def logging_config(self):
        return MyLoggingConfig(level=self.logging_level, filename="asyncmq.log")
```

---

## 5. Detailed Reference

| Component                | Location                                | Description                                        |
| ------------------------ | --------------------------------------- | -------------------------------------------------- |
| `LoggingConfig` Protocol | `asyncmq/logging.py`                    | Interface requiring `level` & `configure()`        |
| `StandardLoggingConfig`  | `asyncmq/core/utils/logging.py`         | Default setup: console handler, timestamped format |
| `logging_config`         | `asyncmq.conf.global_settings.Settings` | Dynamically returns a `LoggingConfig` instance     |

---

## 6. Best Practices & Pitfalls

* **Always configure logging before other imports** to capture early warnings.
* **Avoid multiple handlers**: guard against repeated `basicConfig()` calls by checking `is_logging_setup`.
* **Use structured logging** in production: consider JSON for logs shipped to ELK or Splunk.
* **Be mindful of performance**: expensive formatters or handlers (network/file I/O) can slow down high‑throughput workers.

---

With this knowledge, you can tame logging in AsyncMQ, whether you stick with the standard console output or build a
fully customized observability pipeline! 🚀
