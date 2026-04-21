from __future__ import annotations

from importlib import import_module
from typing import Any

from .config_reader import read_databricks_config

__all__ = [
    "databricks_connection",
    "read_databricks_config",
]


def __getattr__(name: str) -> Any:
    if name == "databricks_connection":
        module = import_module("src.infrastructure.databricks.client")
        return getattr(module, name)
    raise AttributeError(f"module 'src.infrastructure.databricks' has no attribute {name!r}")
