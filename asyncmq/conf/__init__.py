from __future__ import annotations  # Enable postponed evaluation of type hints

import os
from typing import TYPE_CHECKING, Any, Callable, cast

from monkay import Monkay

# Define the environment variable name used to specify a custom settings module.
ENVIRONMENT_VARIABLE = "ASYNCMQ_SETTINGS_MODULE"

# Conditionally import the Settings class only for type checking purposes.
# This prevents a circular dependency during runtime if this module is imported
# before the actual settings module is loaded.
if TYPE_CHECKING:
    # Import the concrete Settings class from the global settings module.
    # This import is only active during static analysis (like type checking)
    # and is ignored at runtime.
    from asyncmq.conf.global_settings import Settings

# Initialize the Monkay instance. Monkay is responsible for loading and managing
# the settings object based on the configuration provided.
# - globals(): Provides the global namespace where settings will be made available.
# - settings_path: A callable that returns the path to the settings object or
#                  callable. It first checks the ENVIRONMENT_VARIABLE for a
#                  custom path, defaulting to 'asyncmq.conf.global_settings.Settings'.
# - with_instance=True: Indicates that the settings_path should point to an
#                       instance or a callable that returns an instance of the
#                       settings object, rather than just the module path.
_monkay: Monkay[Callable[..., Any], Settings] = Monkay(
    globals(),
    settings_path=lambda: os.environ.get(ENVIRONMENT_VARIABLE, "asyncmq.conf.global_settings.Settings"),
    with_instance=True,
)


class SettingsForward:
    """
    A descriptor class that acts as a proxy for the actual settings object
    managed by Monkay.

    This class intercepts attribute access (getting and setting) on an instance
    of itself and forwards these operations to the underlying settings object
    loaded by Monkay. This allows for a dynamic settings object that is loaded
    on first access and can be configured via environment variables.
    """

    def __getattribute__(self, name: str) -> Any:
        """
        Intercepts attribute access (e.g., `settings.DEBUG`).

        This method is called whenever an attribute is accessed on an instance
        of SettingsForward. It retrieves the actual settings object from Monkay
        and returns the requested attribute from it.

        Args:
            name: The name of the attribute being accessed.

        Returns:
            The value of the attribute from the underlying settings object.
        """
        # Retrieve the actual settings object from the Monkay instance and
        # get the requested attribute from it.
        return getattr(_monkay.settings, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Intercepts attribute setting (e.g., `settings.DEBUG = True`).

        This method is called whenever an attribute is set on an instance
        of SettingsForward. It retrieves the actual settings object from Monkay
        and sets the attribute on it with the provided value.

        Args:
            name: The name of the attribute being set.
            value: The value to set the attribute to.
        """
        # Retrieve the actual settings object from the Monkay instance and
        # set the attribute on it with the given value.
        return setattr(_monkay.settings, name, value)


# Create a global settings object that is an instance of SettingsForward.
# This object is the public interface for accessing settings. When attributes
# are accessed on 'settings', the SettingsForward descriptor will intercept
# the call and delegate it to the actual settings object loaded by Monkay.
# The 'cast' is used here to inform type checkers that although 'settings'
# is an instance of SettingsForward, it should be treated as if it were
# an instance of the 'Settings' class for type checking purposes.
settings: Settings = cast("Settings", SettingsForward())
