from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import time
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

import pandas as pd
import streamlit as st


def _load_env_from_file() -> None:
    env_path = Path.cwd() / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value

_ENV_LOADED = False


REQUIRED_OUTPUT_COLUMNS = ["Product Name", "Total Revenue", "Total Sales"]
DEFAULT_SQL_POLL_SECONDS = 1
DEFAULT_SQL_TIMEOUT_SECONDS = 60
DEFAULT_SQL_WAIT_TIMEOUT = "10s"
REQUIRED_ROW_FIELDS = {"product_name", "total_revenue", "total_sales"}


class DatabricksUnavailableError(RuntimeError):
    """Raised when Databricks access is not available in the current environment."""


class DatabricksSchemaError(ValueError):
    """Raised when Databricks rows cannot be mapped to the report contract."""


@dataclass(frozen=True)
class DatabricksConfig:
    host: str
    token: str
    warehouse_id: str
    source_label: str = "sql_warehouse"
    poll_seconds: int = DEFAULT_SQL_POLL_SECONDS
    timeout_seconds: int = DEFAULT_SQL_TIMEOUT_SECONDS
    wait_timeout: str = DEFAULT_SQL_WAIT_TIMEOUT


def _read_env(name: str) -> str:
    return os.getenv(name, "").strip()


def _read_secret(name: str) -> str:
    try:
        secret_value = st.secrets.get(name, "")
    except Exception:
        return ""

    return str(secret_value).strip()


def _read_config_value(name: str) -> str:
    return _read_secret(name) or _read_env(name)


def _load_default_dataset_config() -> dict[str, Any]:
    config_path = Path.cwd() / "config" / "datasets.json"
    if not config_path.exists():
        raise DatabricksUnavailableError(f"Missing dataset configuration file: {config_path}")

    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DatabricksUnavailableError("Dataset configuration file is not valid JSON.") from exc

    if not isinstance(payload, dict):
        raise DatabricksUnavailableError("Dataset configuration must be a JSON object.")

    default_dataset_id = str(payload.get("default_dataset", "")).strip()
    datasets = payload.get("datasets")
    if not default_dataset_id:
        raise DatabricksUnavailableError("Dataset configuration is missing default_dataset.")
    if not isinstance(datasets, list):
        raise DatabricksUnavailableError("Dataset configuration field 'datasets' must be a list.")

    for dataset in datasets:
        if not isinstance(dataset, dict):
            continue
        if str(dataset.get("id", "")).strip() == default_dataset_id:
            return dataset

    raise DatabricksUnavailableError(f"Default dataset '{default_dataset_id}' was not found in config/datasets.json.")


def _build_statement_from_dataset_config(dataset: dict[str, Any], has_product_name_filter: bool) -> str:
    dataset_id = str(dataset.get("id", "")).strip() or "default_dataset"
    base_table = str(dataset.get("base_table", "")).strip()
    base_alias = str(dataset.get("base_alias", "")).strip() or "src"
    columns = dataset.get("columns")

    if not base_table:
        raise DatabricksUnavailableError(f"Dataset '{dataset_id}' is missing base_table in config/datasets.json.")
    if not isinstance(columns, dict):
        raise DatabricksUnavailableError(f"Dataset '{dataset_id}' is missing columns mapping in config/datasets.json.")

    select_parts: list[str] = []
    for output_name in ("product_name", "total_revenue", "total_sales"):
        expression = str(columns.get(output_name, "")).strip()
        if not expression:
            raise DatabricksUnavailableError(
                f"Dataset '{dataset_id}' is missing column mapping for '{output_name}' in config/datasets.json."
            )
        select_parts.append(f"{expression} AS {output_name}")

    statement_lines = [
        "SELECT",
        f"  {', '.join(select_parts)}",
        f"FROM {base_table} AS {base_alias}",
    ]

    if has_product_name_filter:
        product_expression = str(columns["product_name"]).strip()
        statement_lines.append(f"WHERE LOWER(COALESCE({product_expression}, '')) LIKE :product_name_pattern")

    return "\n".join(statement_lines)


def load_databricks_config() -> DatabricksConfig:
    global _ENV_LOADED
    if not _ENV_LOADED:
        _load_env_from_file()
        _ENV_LOADED = True

    values = {
        "DATABRICKS_HOST": _read_config_value("DATABRICKS_HOST"),
        "DATABRICKS_TOKEN": _read_config_value("DATABRICKS_TOKEN"),
        "DATABRICKS_WAREHOUSE_ID": _read_config_value("DATABRICKS_WAREHOUSE_ID"),
    }
    missing = [key for key, value in values.items() if not value]
    if missing:
        joined = ", ".join(missing)
        raise DatabricksUnavailableError(f"Missing Databricks configuration: {joined}")

    try:
        poll_seconds = int(_read_config_value("DATABRICKS_SQL_POLL_SECONDS") or DEFAULT_SQL_POLL_SECONDS)
    except ValueError:
        poll_seconds = DEFAULT_SQL_POLL_SECONDS

    try:
        timeout_seconds = int(
            _read_config_value("DATABRICKS_SQL_TIMEOUT_SECONDS") or DEFAULT_SQL_TIMEOUT_SECONDS
        )
    except ValueError:
        timeout_seconds = DEFAULT_SQL_TIMEOUT_SECONDS

    dataset = _load_default_dataset_config()
    source_label = str(dataset.get("label", "")).strip() or str(dataset.get("id", "")).strip() or "sql_warehouse"
    wait_timeout = _read_config_value("DATABRICKS_SQL_WAIT_TIMEOUT") or DEFAULT_SQL_WAIT_TIMEOUT

    return DatabricksConfig(
        host=values["DATABRICKS_HOST"],
        token=values["DATABRICKS_TOKEN"],
        warehouse_id=values["DATABRICKS_WAREHOUSE_ID"],
        source_label=source_label,
        poll_seconds=max(1, poll_seconds),
        timeout_seconds=max(10, timeout_seconds),
        wait_timeout=wait_timeout,
    )


def _normalize_base_url(host: str) -> str:
    clean = host.strip().rstrip("/")
    if clean.startswith("http://") or clean.startswith("https://"):
        return clean
    return f"https://{clean}"


def _databricks_api_request(base_url: str, token: str, method: str, path: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")

    req = urllib_request.Request(
        url=f"{base_url}{path}",
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib_request.urlopen(req, timeout=60) as response:
            raw_payload = response.read().decode("utf-8")
            return json.loads(raw_payload) if raw_payload else {}
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise DatabricksUnavailableError(f"Databricks API error ({exc.code}): {detail}") from exc
    except urllib_error.URLError as exc:
        raise DatabricksUnavailableError(f"Cannot reach Databricks host: {exc.reason}") from exc


def _build_execute_statement_payload(config: DatabricksConfig, product_name: str) -> dict[str, Any]:
    dataset = _load_default_dataset_config()
    has_product_name_filter = product_name != ""
    statement = _build_statement_from_dataset_config(dataset, has_product_name_filter=has_product_name_filter)
    payload = {
        "statement": statement,
        "warehouse_id": config.warehouse_id,
        "wait_timeout": config.wait_timeout,
        "on_wait_timeout": "CONTINUE",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
    }
    if has_product_name_filter:
        payload["parameters"] = [
            {
                "name": "product_name_pattern",
                "value": f"%{product_name.lower()}%",
                "type": "STRING",
            }
        ]
    return payload


def _poll_statement_completion(
    base_url: str,
    token: str,
    statement_id: str,
    poll_seconds: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        response = _databricks_api_request(
            base_url=base_url,
            token=token,
            method="GET",
            path=f"/api/2.0/sql/statements/{statement_id}",
            payload=None,
        )
        status = response.get("status", {}) if isinstance(response, dict) else {}
        state = str(status.get("state", "")).upper()
        error_message = str(status.get("error", {}).get("message", "")).strip() if isinstance(status, dict) else ""

        if state == "SUCCEEDED":
            return response
        if state in {"FAILED", "CANCELED", "CLOSED"}:
            suffix = f" {error_message}" if error_message else ""
            raise DatabricksUnavailableError(f"Databricks SQL statement {state.lower()}.{suffix}".strip())

        time.sleep(poll_seconds)

    raise DatabricksUnavailableError(f"Databricks SQL statement {statement_id} timed out after {timeout_seconds}s")


def _parse_statement_result_rows(statement_response: dict[str, Any]) -> list[dict[str, Any]]:
    manifest = statement_response.get("manifest", {}) if isinstance(statement_response, dict) else {}
    result = statement_response.get("result", {}) if isinstance(statement_response, dict) else {}
    schema = manifest.get("schema", {}) if isinstance(manifest, dict) else {}
    columns = schema.get("columns", []) if isinstance(schema, dict) else []

    if not isinstance(columns, list) or not columns:
        raise DatabricksSchemaError("Databricks SQL response is missing manifest.schema.columns.")

    column_names = [str(column.get("name", "")).strip() for column in columns if isinstance(column, dict)]
    if not REQUIRED_ROW_FIELDS.issubset(set(column_names)):
        raise DatabricksSchemaError("Databricks SQL response is missing required columns: product_name, total_revenue, total_sales")

    raw_rows = result.get("data_array", []) if isinstance(result, dict) else []
    if raw_rows is None:
        raw_rows = []
    if not isinstance(raw_rows, list):
        raise DatabricksSchemaError("Databricks SQL result.data_array must be a list.")

    normalized_rows: list[dict[str, Any]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, list):
            raise DatabricksSchemaError("Each Databricks SQL row must be a list.")
        if len(raw_row) != len(column_names):
            raise DatabricksSchemaError("Databricks SQL row length does not match manifest schema columns.")
        row_map = dict(zip(column_names, raw_row))
        normalized_rows.append(
            {
                "product_name": row_map.get("product_name"),
                "total_revenue": row_map.get("total_revenue"),
                "total_sales": row_map.get("total_sales"),
            }
        )
    return normalized_rows


def _collect_statement_rows(base_url: str, token: str, statement_response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _parse_statement_result_rows(statement_response)
    if rows:
        return rows

    statement_id = str(statement_response.get("statement_id", "")).strip()
    manifest = statement_response.get("manifest", {}) if isinstance(statement_response, dict) else {}
    total_chunks = int(manifest.get("total_chunk_count", 0) or 0) if isinstance(manifest, dict) else 0

    if statement_id and total_chunks > 1:
        collected_rows: list[dict[str, Any]] = []
        for chunk_index in range(total_chunks):
            chunk_response = _databricks_api_request(
                base_url=base_url,
                token=token,
                method="GET",
                path=f"/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}",
                payload=None,
            )
            collected_rows.extend(_parse_statement_result_rows({"manifest": manifest, "result": chunk_response}))
        return collected_rows

    return rows


def fetch_databricks_rows(
    config: DatabricksConfig,
    product_name: str = "",
) -> list[dict[str, Any]]:
    base_url = _normalize_base_url(config.host)
    execute_payload = _build_execute_statement_payload(config, product_name=product_name)

    statement_response = _databricks_api_request(
        base_url=base_url,
        token=config.token,
        method="POST",
        path="/api/2.0/sql/statements/",
        payload=execute_payload,
    )

    statement_id = str(statement_response.get("statement_id", "")).strip()
    if not statement_id:
        raise DatabricksUnavailableError("Databricks SQL response is missing statement_id.")

    status = statement_response.get("status", {}) if isinstance(statement_response, dict) else {}
    state = str(status.get("state", "")).upper() if isinstance(status, dict) else ""
    if state != "SUCCEEDED":
        statement_response = _poll_statement_completion(
            base_url=base_url,
            token=config.token,
            statement_id=statement_id,
            poll_seconds=config.poll_seconds,
            timeout_seconds=config.timeout_seconds,
        )

    return _collect_statement_rows(
        base_url=base_url,
        token=config.token,
        statement_response=statement_response,
    )


def normalize_databricks_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=REQUIRED_OUTPUT_COLUMNS)

    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not REQUIRED_ROW_FIELDS.issubset(row.keys()):
            raise DatabricksSchemaError(
                "Databricks row is missing required fields: product_name, total_revenue, total_sales"
            )

        product_name = str(row["product_name"]).strip()
        if not product_name:
            raise DatabricksSchemaError("product_name cannot be empty.")

        try:
            total_revenue = float(row["total_revenue"])
        except (TypeError, ValueError) as exc:
            raise DatabricksSchemaError("total_revenue must be numeric.") from exc

        try:
            total_sales = int(row["total_sales"])
        except (TypeError, ValueError) as exc:
            raise DatabricksSchemaError("total_sales must be numeric/integer-compatible.") from exc

        normalized.append(
            {
                "Product Name": product_name,
                "Total Revenue": total_revenue,
                "Total Sales": total_sales,
            }
        )

    return pd.DataFrame(normalized, columns=REQUIRED_OUTPUT_COLUMNS)
