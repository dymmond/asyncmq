from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Any, cast

from monkay import Monkay

if TYPE_CHECKING:
    from .global_settings import Settings


@lru_cache
def get_asyncmq_monkay() -> Monkay[None, Settings]:
    from asyncmq import monkay

    monkay.evaluate_settings(ignore_import_errors=False)
    return monkay


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
        monkay = get_asyncmq_monkay()
        return getattr(monkay.settings, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Intercepts attribute set.

        This method is called whenever an attribute is set on the instance
        of SettingsForward. It retrieves the actual settings object from Monkay
        and sets the requested attribute.

        Args:
            name: The name of the attribute being set.
            value: The value passed.
        """
        monkay = get_asyncmq_monkay()
        setattr(monkay.settings, name, value)

    def __delattr__(self, name: str) -> None:
        """
        Intercepts attribute delete.

        This method is called whenever an attribute is deleted on the instance
        of SettingsForward. It retrieves the actual settings object from Monkay
        and deletes the requested attribute.

        Args:
            name: The name of the attribute being set.
            value: The value passed.
        """
        monkay = get_asyncmq_monkay()
        delattr(monkay.settings, name)


settings = cast("Settings", SettingsForward())
