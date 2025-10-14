from __future__ import annotations

from functools import lru_cache
from typing import Any

from monkay import Monkay


@lru_cache
def get_asyncmq_monkay() -> Monkay[None, Any]:
    from asyncmq import monkay

    monkay.evaluate_settings(on_conflict="error", ignore_import_errors=False)
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


settings = SettingsForward()
