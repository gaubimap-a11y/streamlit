from __future__ import annotations

from src.core.config import DatabricksConfig, get_settings


def read_databricks_config() -> DatabricksConfig:
    return get_settings().databricks
