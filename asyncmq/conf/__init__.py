from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Callable, cast

from monkay import Monkay

# Define the environment variable name used to specify custom settings module.
ENVIRONMENT_VARIABLE = "ASYNCMQ_SETTINGS_MODULE"

# Conditionally import the Settings class only for type checking purposes.
# This avoids a circular dependency during runtime if this module is imported
# before the actual settings module.
if TYPE_CHECKING:
    from asyncmq.conf.global_settings import Settings

# Initialize the Monkay instance which is responsible for loading and managing
# the settings object.
# - It's configured to find settings based on the ENVIRONMENT_VARIABLE or
#   defaulting to 'asyncmq.conf.global_settings.Settings'.
# - 'with_instance=True' means it expects the settings path to point to
#   an instance or callable that returns an instance.
_monkay: Monkay[Callable[..., Any], Settings] = Monkay(
    globals(),
    settings_path=lambda: os.environ.get(ENVIRONMENT_VARIABLE, "asyncmq.conf.global_settings.Settings"),
    with_instance=True,
)


class SettingsForward:
    """
    A descriptor class that forwards attribute access and setting
    to the underlying settings object managed by Monkay.

    This allows accessing settings like `settings.DEBUG` even though
    `settings` itself is this forwarding object, not the actual settings instance.
    It provides a dynamic way to access the loaded settings.
    """

    def __getattribute__(self, name: str) -> Any:
        """
        Intercepts attribute access and forwards it to the Monkay-loaded settings.
        """
        # Get the attribute from the Monkay-managed settings object.
        return getattr(_monkay.settings, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Intercepts attribute setting and forwards it to the Monkay-loaded settings.
        """
        # Set the attribute on the Monkay-managed settings object.
        return setattr(_monkay.settings, name, value)


# Create a global settings object that is an instance of SettingsForward.
# This object will dynamically load settings via Monkay on first access
# and forward all attribute operations to the loaded settings.
settings: Settings = cast("Settings", SettingsForward())
