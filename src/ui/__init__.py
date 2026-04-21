from __future__ import annotations

from importlib import import_module
from typing import Any


_LAZY_EXPORTS = {
    "DashboardPage": "src.ui.pages.dashboard_page",
    "LoginPage": "src.ui.pages.login_page",
    "UsersPage": "src.ui.pages.users_page",
}

__all__ = ["DashboardPage", "LoginPage"]


def __getattr__(name: str) -> Any:
    module_path = _LAZY_EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_path)
    return getattr(module, name)
