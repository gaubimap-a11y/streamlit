from __future__ import annotations

import configparser
from pathlib import Path

from src.infrastructure.repositories import sql_warehouse_source as source


def _make_cfg(
    *,
    host: str,
    http_path: str,
    token: str,
    source_label: str = "",
    poll_seconds: str = "",
    timeout_seconds: str = "",
    wait_timeout: str = "",
) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config["databricks"] = {
        "server_hostname": host,
        "http_path": http_path,
        "access_token": token,
        "source_label": source_label,
        "poll_seconds": poll_seconds,
        "timeout_seconds": timeout_seconds,
        "wait_timeout": wait_timeout,
    }
    return config


def test_load_databricks_config_reads_cfg_and_ignores_env(mocker, monkeypatch):
    cfg = _make_cfg(
        host="dbc-local.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/local123",
        token="token-local",
        source_label="local_cfg",
        poll_seconds="2",
        timeout_seconds="30",
        wait_timeout="15s",
    )
    mocker.patch(
        "src.infrastructure.repositories.sql_warehouse_source._load_databricks_cfg",
        return_value=(cfg, Path("databricks.local.cfg")),
    )
    monkeypatch.setenv("DATABRICKS_HOST", "env-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "env-warehouse")

    config = source.load_databricks_config()

    assert config.host == "dbc-local.cloud.databricks.com"
    assert config.token == "token-local"
    assert config.warehouse_id == "local123"
    assert config.source_label == "local_cfg"
    assert config.poll_seconds == 2
    assert config.timeout_seconds == 30
    assert config.wait_timeout == "15s"


def test_load_databricks_config_falls_back_to_template_cfg(mocker):
    cfg = _make_cfg(
        host="dbc-template.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/template123",
        token="token-template",
    )
    mocker.patch(
        "src.infrastructure.repositories.sql_warehouse_source._load_databricks_cfg",
        return_value=(cfg, Path("databricks.cfg")),
    )

    config = source.load_databricks_config()

    assert config.host == "dbc-template.cloud.databricks.com"
    assert config.token == "token-template"
    assert config.warehouse_id == "template123"
    assert config.source_label == "sql_warehouse"
