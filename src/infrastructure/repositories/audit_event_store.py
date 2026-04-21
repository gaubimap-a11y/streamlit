from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.domain.audit_models import AuditEvent
from src.domain.auth_validation import AuthSystemUnavailableError
from src.infrastructure.repositories.sql_warehouse_source import (
    DatabricksConfig,
    DatabricksUnavailableError,
    _databricks_api_request,
    _normalize_base_url,
    _poll_statement_completion,
    load_databricks_config,
)

AUDIT_TABLE_NAME = "tmn_kobe.auth.app_audit_events"


@dataclass(frozen=True)
class DatabricksAuditEventStore:
    config: DatabricksConfig

    @classmethod
    def from_current_config(cls) -> "DatabricksAuditEventStore":
        return cls(config=load_databricks_config())

    def write_event(self, event: AuditEvent) -> None:
        details_json = event.as_record()["details_json"]
        statement = "\n".join(
            [
                f"INSERT INTO {AUDIT_TABLE_NAME} (",
                "  event_id, event_type, occurred_at, principal_id, auth_source, resource, action, result, correlation_id, details_json",
                ") VALUES (",
                "  :event_id, :event_type, CAST(:occurred_at AS TIMESTAMP), :principal_id, :auth_source, :resource, :action, :result, :correlation_id, :details_json",
                ")",
            ]
        )
        self._execute_statement(
            statement,
            parameters=[
                {"name": "event_id", "value": event.event_id, "type": "STRING"},
                {"name": "event_type", "value": event.event_type, "type": "STRING"},
                {"name": "occurred_at", "value": event.occurred_at.isoformat(sep=" "), "type": "STRING"},
                {"name": "principal_id", "value": event.principal_id, "type": "STRING"},
                {"name": "auth_source", "value": event.auth_source, "type": "STRING"},
                {"name": "resource", "value": event.resource, "type": "STRING"},
                {"name": "action", "value": event.action, "type": "STRING"},
                {"name": "result", "value": event.result, "type": "STRING"},
                {"name": "correlation_id", "value": event.correlation_id, "type": "STRING"},
                {"name": "details_json", "value": details_json, "type": "STRING"},
            ],
        )

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
                raise AuthSystemUnavailableError("Databricks audit response is missing statement_id.")

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
