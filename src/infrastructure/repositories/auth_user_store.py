from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from werkzeug.security import check_password_hash, generate_password_hash

from src.domain.auth_models import AuthUserRecord
from src.domain.auth_validation import AuthSystemUnavailableError
from src.infrastructure.repositories.sql_warehouse_source import (
    DatabricksConfig,
    DatabricksUnavailableError,
    _databricks_api_request,
    _normalize_base_url,
    _poll_statement_completion,
    load_databricks_config,
)

USER_TABLE_NAME = "tmn_kobe.auth.users"
USER_SELECT_COLUMNS = [
    "user_id",
    "username",
    "email",
    "password_hash",
    "created_at",
    "last_login_at",
    "is_active",
    "display_name",
]


@dataclass(frozen=True)
class DatabricksAuthUserStore:
    config: DatabricksConfig

    @classmethod
    def from_current_config(cls) -> "DatabricksAuthUserStore":
        return cls(config=load_databricks_config())

    def get_user_by_username(self, username: str) -> AuthUserRecord | None:
        statement = "\n".join(
            [
                "SELECT",
                "  user_id, username, email, password_hash, created_at, last_login_at, is_active,",
                "  COALESCE(display_name, username) AS display_name",
                f"FROM {USER_TABLE_NAME}",
                "WHERE LOWER(username) = LOWER(:username)",
                "  AND LOWER(COALESCE(auth_source, 'internal')) = 'internal'",
                "  AND COALESCE(is_deleted, false) = false",
                "  AND COALESCE(password_hash, '') <> ''",
                "LIMIT 1",
            ]
        )
        rows = self._execute_select(
            statement,
            parameters=[{"name": "username", "value": username, "type": "STRING"}],
        )
        if not rows:
            return None
        return _map_auth_user_record(rows[0])

    def update_last_login(self, user_id: str, logged_in_at: datetime) -> None:
        statement = "\n".join(
            [
                f"UPDATE {USER_TABLE_NAME}",
                "SET last_login_at = CAST(:last_login_at AS TIMESTAMP)",
                "  , updated_at = current_timestamp()",
                "WHERE user_id = :user_id",
            ]
        )
        self._execute_statement(
            statement,
            parameters=[
                {"name": "last_login_at", "value": logged_in_at.isoformat(sep=" "), "type": "STRING"},
                {"name": "user_id", "value": user_id, "type": "STRING"},
            ],
        )

    def verify_password(self, plain_password: str, password_hash: str) -> bool:
        try:
            return check_password_hash(password_hash, plain_password)
        except ValueError as exc:
            raise AuthSystemUnavailableError("Stored password hash is not compatible with werkzeug.") from exc

    def hash_password(self, plain_password: str) -> str:
        try:
            return generate_password_hash(plain_password)
        except ValueError as exc:
            raise AuthSystemUnavailableError("Password value is not compatible with werkzeug.") from exc

    def _execute_select(self, statement: str, parameters: list[dict[str, str]]) -> list[dict[str, Any]]:
        response = self._execute_statement(statement, parameters=parameters)
        return _parse_select_rows(response)

    def _execute_statement(self, statement: str, parameters: list[dict[str, str]] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "statement": statement,
            "warehouse_id": self.config.warehouse_id,
            "wait_timeout": self.config.wait_timeout,
            "on_wait_timeout": "CONTINUE",
            "format": "JSON_ARRAY",
            "disposition": "INLINE",
        }
        if parameters:
            payload["parameters"] = parameters

        base_url = _normalize_base_url(self.config.host)
        try:
            response = _databricks_api_request(
                base_url=base_url,
                token=self.config.token,
                method="POST",
                path="/api/2.0/sql/statements/",
                payload=payload,
            )
            statement_id = str(response.get("statement_id", "")).strip()
            if not statement_id:
                raise AuthSystemUnavailableError("Databricks SQL response is missing statement_id.")

            status = response.get("status", {}) if isinstance(response, dict) else {}
            state = str(status.get("state", "")).upper() if isinstance(status, dict) else ""
            if state != "SUCCEEDED":
                response = _poll_statement_completion(
                    base_url=base_url,
                    token=self.config.token,
                    statement_id=statement_id,
                    poll_seconds=self.config.poll_seconds,
                    timeout_seconds=self.config.timeout_seconds,
                )
            return response
        except DatabricksUnavailableError as exc:
            raise AuthSystemUnavailableError(str(exc)) from exc


def _parse_select_rows(statement_response: dict[str, Any]) -> list[dict[str, Any]]:
    manifest = statement_response.get("manifest", {}) if isinstance(statement_response, dict) else {}
    result = statement_response.get("result", {}) if isinstance(statement_response, dict) else {}
    schema = manifest.get("schema", {}) if isinstance(manifest, dict) else {}
    columns = schema.get("columns", []) if isinstance(schema, dict) else []
    raw_rows = result.get("data_array", []) if isinstance(result, dict) else []

    if not isinstance(columns, list) or not columns:
        raise AuthSystemUnavailableError("Databricks auth query response is missing schema columns.")
    if raw_rows is None:
        raw_rows = []
    if not isinstance(raw_rows, list):
        raise AuthSystemUnavailableError("Databricks auth query rows must be a list.")

    column_names = [str(column.get("name", "")).strip() for column in columns if isinstance(column, dict)]
    missing_columns = [name for name in USER_SELECT_COLUMNS if name not in column_names]
    if missing_columns:
        joined = ", ".join(missing_columns)
        raise AuthSystemUnavailableError(f"Databricks auth query is missing required columns: {joined}")

    parsed_rows: list[dict[str, Any]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, list):
            raise AuthSystemUnavailableError("Databricks auth query row must be a list.")
        if len(raw_row) != len(column_names):
            raise AuthSystemUnavailableError("Databricks auth query row length does not match schema columns.")
        parsed_rows.append(dict(zip(column_names, raw_row)))
    return parsed_rows


def _map_auth_user_record(row: dict[str, Any]) -> AuthUserRecord:
    return AuthUserRecord(
        user_id=str(row["user_id"]),
        username=str(row["username"]),
        email=str(row["email"]),
        password_hash=str(row["password_hash"]),
        created_at=_parse_optional_datetime(row.get("created_at")),
        last_login_at=_parse_optional_datetime(row.get("last_login_at")),
        is_active=_parse_bool(row.get("is_active")),
        display_name=str(row.get("display_name") or row.get("username", "")).strip(),
    )


def _parse_optional_datetime(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value

    normalized = str(value).strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized)


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise AuthSystemUnavailableError("Databricks auth query returned invalid is_active value.")
