from __future__ import annotations  # Enable postponed evaluation of type hints

import os
from typing import TYPE_CHECKING, Any, cast

from monkay import Monkay

ENVIRONMENT_VARIABLE = "ASYNCMQ_SETTINGS_MODULE"

if TYPE_CHECKING:
    from asyncmq.conf.global_settings import Settings

monkay: Monkay[None, Settings] = Monkay(
    globals(),
    settings_path=lambda: os.environ.get(ENVIRONMENT_VARIABLE, "asyncmq.conf.global_settings.Settings"),
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
        Intercepts attribute access (e.g., `monkay.settings.DEBUG`).

        This method is called whenever an attribute is accessed on an instance
        of SettingsForward. It retrieves the actual settings object from Monkay
        and returns the requested attribute from it.

        Args:
            name: The name of the attribute being accessed.

        Returns:
            The value of the attribute from the underlying settings object.
        """
        return getattr(monkay.settings, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Intercepts attribute setting (e.g., `monkay.settings.DEBUG = True`).

        This method is called whenever an attribute is set on an instance
        of SettingsForward. It retrieves the actual settings object from Monkay
        and sets the attribute on it with the provided value.

        Args:
            name: The name of the attribute being set.
            value: The value to set the attribute to.
        """
        return setattr(monkay.settings, name, value)


settings: Settings = cast("Settings", SettingsForward())
