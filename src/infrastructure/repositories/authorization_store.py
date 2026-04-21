from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.domain.auth_validation import AuthSystemUnavailableError
from src.infrastructure.repositories.sql_warehouse_source import (
    DatabricksConfig,
    DatabricksUnavailableError,
    _databricks_api_request,
    _normalize_base_url,
    _poll_statement_completion,
    load_databricks_config,
)

AUTH_SCHEMA_NAME = "tmn_kobe.auth"


@dataclass(frozen=True)
class DatabricksAuthorizationStore:
    config: DatabricksConfig

    @classmethod
    def from_current_config(cls) -> "DatabricksAuthorizationStore":
        return cls(config=load_databricks_config())

    def has_principal_mapping(
        self,
        *,
        principal_id: str,
        username: str,
        email: str,
        auth_source: str,
    ) -> bool:
        statement = "\n".join(
            [
                "SELECT 1 AS mapped",
                f"FROM {AUTH_SCHEMA_NAME}.users u",
                "WHERE COALESCE(u.is_deleted, false) = false",
                "  AND COALESCE(u.is_active, true) = true",
                "  AND (",
                "        u.user_id = :principal_id",
                "     OR LOWER(u.username) = LOWER(:username)",
                "     OR LOWER(u.email) = LOWER(:email)",
                "  )",
                "  AND LOWER(COALESCE(u.auth_source, 'internal')) = LOWER(:auth_source)",
                "LIMIT 1",
            ]
        )
        rows = self._execute_select(
            statement,
            parameters=[
                {"name": "principal_id", "value": principal_id, "type": "STRING"},
                {"name": "username", "value": username, "type": "STRING"},
                {"name": "email", "value": email, "type": "STRING"},
                {"name": "auth_source", "value": auth_source, "type": "STRING"},
            ],
        )
        return bool(rows)

    def upsert_basic_sso_principal(
        self,
        *,
        principal_id: str,
        username: str,
        email: str,
        display_name: str,
        password_hash: str,
    ) -> None:
        principal_statement = "\n".join(
            [
                f"MERGE INTO {AUTH_SCHEMA_NAME}.users AS target",
                "USING (",
                "    SELECT",
                "        :principal_id AS user_id,",
                "        :username AS username,",
                "        :email AS email,",
                "        :display_name AS display_name,",
                "        :password_hash AS password_hash,",
                "        current_timestamp() AS last_login_at,",
                "        'sso' AS auth_source",
                ") AS source",
                "ON target.user_id = source.user_id",
                "WHEN MATCHED THEN UPDATE SET",
                "    target.username = source.username,",
                "    target.email = source.email,",
                "    target.display_name = source.display_name,",
                "    target.password_hash = source.password_hash,",
                "    target.auth_source = source.auth_source,",
                "    target.updated_at = current_timestamp(),",
                "    target.last_login_at = source.last_login_at",
                "WHEN NOT MATCHED THEN INSERT (",
                "    user_id,",
                "    username,",
                "    email,",
                "    display_name,",
                "    password_hash,",
                "    auth_source,",
                "    last_login_at,",
                "    is_active,",
                "    created_at,",
                "    updated_at,",
                "    deleted_at,",
                "    created_by,",
                "    updated_by,",
                "    deleted_by,",
                "    is_deleted",
                ") VALUES (",
                "    source.user_id,",
                "    source.username,",
                "    source.email,",
                "    source.display_name,",
                "    source.password_hash,",
                "    source.auth_source,",
                "    source.last_login_at,",
                "    true,",
                "    current_timestamp(),",
                "    current_timestamp(),",
                "    NULL,",
                "    'sso-self-service',",
                "    'sso-self-service',",
                "    NULL,",
                "    false",
                ")",
            ]
        )
        self._execute_statement(
            principal_statement,
            parameters=[
                {"name": "principal_id", "value": principal_id, "type": "STRING"},
                {"name": "username", "value": username, "type": "STRING"},
                {"name": "email", "value": email, "type": "STRING"},
                {"name": "display_name", "value": display_name, "type": "STRING"},
                {"name": "password_hash", "value": password_hash, "type": "STRING"},
            ],
        )

        role_statement = "\n".join(
            [
                f"MERGE INTO {AUTH_SCHEMA_NAME}.user_roles AS target",
                "USING (",
                "    SELECT",
                "        :principal_id AS user_id,",
                "        'R004' AS role_id",
                ") AS source",
                "ON target.user_id = source.user_id",
                "   AND target.role_id = source.role_id",
                "WHEN MATCHED THEN UPDATE SET",
                "    target.updated_at = current_timestamp(),",
                "    target.updated_by = 'sso-self-service',",
                "    target.deleted_at = NULL,",
                "    target.deleted_by = NULL,",
                "    target.is_deleted = false",
                "WHEN NOT MATCHED THEN INSERT (",
                "    user_id,",
                "    role_id,",
                "    created_at,",
                "    updated_at,",
                "    deleted_at,",
                "    created_by,",
                "    updated_by,",
                "    deleted_by,",
                "    is_deleted",
                ") VALUES (",
                "    source.user_id,",
                "    source.role_id,",
                "    current_timestamp(),",
                "    current_timestamp(),",
                "    NULL,",
                "    'sso-self-service',",
                "    'sso-self-service',",
                "    NULL,",
                "    false",
                ")",
            ]
        )
        self._execute_statement(
            role_statement,
            parameters=[{"name": "principal_id", "value": principal_id, "type": "STRING"}],
        )

    def resolve_permissions(
        self,
        *,
        principal_id: str,
        username: str,
        email: str,
        auth_source: str,
    ) -> tuple[str, ...]:
        statement = "\n".join(
            [
                "SELECT DISTINCT perm.permission_name",
                f"FROM {AUTH_SCHEMA_NAME}.users u",
                f"INNER JOIN {AUTH_SCHEMA_NAME}.user_roles user_role",
                "  ON u.user_id = user_role.user_id",
                "  AND COALESCE(user_role.is_deleted, false) = false",
                f"INNER JOIN {AUTH_SCHEMA_NAME}.roles r",
                "  ON user_role.role_id = r.role_id",
                "  AND r.is_active = TRUE",
                "  AND COALESCE(r.is_deleted, false) = false",
                f"INNER JOIN {AUTH_SCHEMA_NAME}.role_permissions role_permission",
                "  ON r.role_id = role_permission.role_id",
                "  AND COALESCE(role_permission.is_deleted, false) = false",
                f"INNER JOIN {AUTH_SCHEMA_NAME}.permissions perm",
                "  ON role_permission.permission_id = perm.permission_id",
                "  AND perm.is_active = TRUE",
                "  AND COALESCE(perm.is_deleted, false) = false",
                "WHERE u.is_active = TRUE",
                "  AND COALESCE(u.is_deleted, false) = false",
                "  AND (",
                "        u.user_id = :principal_id",
                "     OR LOWER(u.username) = LOWER(:username)",
                "     OR LOWER(u.email) = LOWER(:email)",
                "  )",
                "  AND LOWER(COALESCE(u.auth_source, 'internal')) = LOWER(:auth_source)",
                "ORDER BY perm.permission_name",
            ]
        )
        rows = self._execute_select(
            statement,
            parameters=[
                {"name": "principal_id", "value": principal_id, "type": "STRING"},
                {"name": "username", "value": username, "type": "STRING"},
                {"name": "email", "value": email, "type": "STRING"},
                {"name": "auth_source", "value": auth_source, "type": "STRING"},
            ],
        )
        permissions: list[str] = []
        for row in rows:
            value = row.get("permission_name")
            if value is not None:
                permission_name = str(value).strip()
                if permission_name and permission_name not in permissions:
                    permissions.append(permission_name)
        return tuple(permissions)

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
                raise AuthSystemUnavailableError("Databricks authorization response is missing statement_id.")

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
            _raise_for_unsuccessful_statement(response, context="Databricks authorization query")
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
        raise AuthSystemUnavailableError("Databricks authorization response is missing schema columns.")
    if raw_rows is None:
        raw_rows = []
    if not isinstance(raw_rows, list):
        raise AuthSystemUnavailableError("Databricks authorization rows must be a list.")

    column_names = [str(column.get("name", "")).strip() for column in columns if isinstance(column, dict)]
    parsed_rows: list[dict[str, Any]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, list):
            raise AuthSystemUnavailableError("Databricks authorization row must be a list.")
        if len(raw_row) != len(column_names):
            raise AuthSystemUnavailableError("Databricks authorization row length does not match schema columns.")
        parsed_rows.append(dict(zip(column_names, raw_row)))
    return parsed_rows


def _raise_for_unsuccessful_statement(response: dict[str, Any], *, context: str) -> None:
    status = response.get("status", {}) if isinstance(response, dict) else {}
    state = str(status.get("state", "")).upper() if isinstance(status, dict) else ""
    if state in {"", "SUCCEEDED"}:
        return
    error = status.get("error", {}) if isinstance(status, dict) else {}
    error_message = ""
    if isinstance(error, dict):
        error_message = str(error.get("message", "")).strip()
    if not error_message:
        error_message = str(status.get("state_message", "")).strip() if isinstance(status, dict) else ""
    details = f": {error_message}" if error_message else ""
    raise AuthSystemUnavailableError(f"{context} failed with state {state}{details}")
