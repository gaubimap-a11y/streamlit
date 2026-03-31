from __future__ import annotations

import pytest

from infrastructure.sql_warehouse_source import (
    DatabricksConfig,
    DatabricksSchemaError,
    _build_execute_statement_payload,
    _build_statement_from_dataset_config,
    _load_default_dataset_config,
    _parse_statement_result_rows,
    fetch_databricks_rows,
    load_databricks_config,
    normalize_databricks_rows,
)


def test_normalize_databricks_rows_maps_contract_columns() -> None:
    rows = [
        {"product_name": "Coffee A", "total_revenue": 1234.5, "total_sales": 9},
        {"product_name": "Coffee A", "total_revenue": "456.7", "total_sales": "3"},
    ]

    df = normalize_databricks_rows(rows)

    assert list(df.columns) == ["Product Name", "Total Revenue", "Total Sales"]
    assert df["Product Name"].tolist() == ["Coffee A", "Coffee A"]
    assert df["Total Revenue"].tolist() == [1234.5, 456.7]
    assert df["Total Sales"].tolist() == [9, 3]


def test_load_default_dataset_config_reads_default_dataset() -> None:
    dataset = _load_default_dataset_config()

    assert dataset["id"] == "gold_drink_sales"
    assert dataset["base_table"] == "workspace.default.gold_drink_sales"


def test_build_statement_from_dataset_config_uses_optional_product_filter() -> None:
    dataset = _load_default_dataset_config()

    statement = _build_statement_from_dataset_config(dataset, has_product_name_filter=True)

    assert "FROM workspace.default.gold_drink_sales AS s" in statement
    assert "s.product_name AS product_name" in statement
    assert "WHERE LOWER(COALESCE(s.product_name, '')) LIKE :product_name_pattern" in statement


def test_build_execute_statement_payload_uses_warehouse_id() -> None:
    config = DatabricksConfig(host="host", token="token", warehouse_id="warehouse-123")

    payload = _build_execute_statement_payload(
        config=config,
        product_name="Coffee A",
    )

    assert payload["warehouse_id"] == "warehouse-123"
    assert payload["format"] == "JSON_ARRAY"
    assert "WHERE LOWER(COALESCE(s.product_name, '')) LIKE :product_name_pattern" in payload["statement"]
    assert payload["parameters"] == [{"name": "product_name_pattern", "value": "%coffee a%", "type": "STRING"}]


def test_build_execute_statement_payload_preserves_whitespace_and_symbols() -> None:
    config = DatabricksConfig(host="host", token="token", warehouse_id="warehouse-123")

    payload = _build_execute_statement_payload(
        config=config,
        product_name="  Ca phe sua da  ",
    )

    assert payload["parameters"] == [{"name": "product_name_pattern", "value": "%  ca phe sua da  %", "type": "STRING"}]


def test_parse_statement_result_rows_maps_data_array_to_row_objects() -> None:
    rows = _parse_statement_result_rows(
        {
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "product_name"},
                        {"name": "total_revenue"},
                        {"name": "total_sales"},
                    ]
                }
            },
            "result": {
                "data_array": [
                    ["LATTE", 45000.0, 1],
                    ["AMERICANO", 90000.0, 3],
                ]
            },
        }
    )

    assert [row["product_name"] for row in rows] == ["LATTE", "AMERICANO"]


def test_parse_statement_result_rows_rejects_missing_required_columns() -> None:
    with pytest.raises(DatabricksSchemaError, match="missing required columns"):
        _parse_statement_result_rows(
            {
                "manifest": {
                    "schema": {
                        "columns": [
                            {"name": "product_name"},
                            {"name": "total_revenue"},
                        ]
                    }
                },
                "result": {"data_array": [["LATTE", 45000.0]]},
            }
        )


def test_load_databricks_config_reads_warehouse_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "token-value")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "warehouse-123")

    config = load_databricks_config()

    assert config.host == "https://example.cloud.databricks.com"
    assert config.token == "token-value"
    assert config.warehouse_id == "warehouse-123"


def test_fetch_databricks_rows_supports_polling_statement_api(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DatabricksConfig(host="https://example.cloud.databricks.com", token="token", warehouse_id="warehouse-123")

    def _fake_request(base_url: str, token: str, method: str, path: str, payload):
        if path == "/api/2.0/sql/statements/":
            return {"statement_id": "stmt-1", "status": {"state": "PENDING"}}
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr("infrastructure.sql_warehouse_source._databricks_api_request", _fake_request)
    monkeypatch.setattr(
        "infrastructure.sql_warehouse_source._poll_statement_completion",
        lambda **kwargs: {
            "statement_id": "stmt-1",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "product_name"},
                        {"name": "total_revenue"},
                        {"name": "total_sales"},
                    ]
                }
            },
            "result": {"data_array": [["Coffee A", 100.0, 2]]},
        },
    )

    rows = fetch_databricks_rows(config, product_name="Coffee A")

    assert rows == [{"product_name": "Coffee A", "total_revenue": 100.0, "total_sales": 2}]
