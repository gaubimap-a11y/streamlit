from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from src.domain.audit_models import AuditEvent, create_audit_event_id
from src.domain.auth_models import AuthenticatedSession
SENSITIVE_DETAIL_KEYS = {
    "password",
    "password_hash",
    "token",
    "signed_cookie",
    "cookie",
    "secret",
    "connection_string",
}


class AuditEventWriter(Protocol):
    def write_event(self, event: AuditEvent) -> None: ...


@dataclass(frozen=True)
class NoOpAuditEventWriter:
    def write_event(self, event: AuditEvent) -> None:
        return None


def build_audit_event(
    *,
    event_type: str,
    principal_id: str,
    auth_source: str,
    resource: str,
    action: str,
    result: str,
    occurred_at: datetime | None = None,
    correlation_id: str = "",
    details: Mapping[str, object] | None = None,
) -> AuditEvent:
    return AuditEvent(
        event_id=create_audit_event_id(),
        event_type=event_type,
        occurred_at=occurred_at or datetime.now(),
        principal_id=principal_id or "anonymous",
        auth_source=auth_source,
        resource=resource,
        action=action,
        result=result,
        correlation_id=correlation_id,
        details=_normalize_details(details),
    )


def build_audit_event_from_session(
    session: AuthenticatedSession | None,
    *,
    event_type: str,
    resource: str,
    action: str,
    result: str,
    occurred_at: datetime | None = None,
    details: Mapping[str, object] | None = None,
) -> AuditEvent:
    if session is None:
        return build_audit_event(
            event_type=event_type,
            principal_id="anonymous",
            auth_source="unknown",
            resource=resource,
            action=action,
            result=result,
            occurred_at=occurred_at,
            details=details,
        )

    return build_audit_event(
        event_type=event_type,
        principal_id=session.principal_id,
        auth_source=session.auth_source,
        resource=resource,
        action=action,
        result=result,
        occurred_at=occurred_at,
        correlation_id=session.correlation_id,
        details=details,
    )


def record_audit_event(writer: AuditEventWriter | None, event: AuditEvent) -> bool:
    if writer is None:
        return False

    try:
        writer.write_event(event)
    except Exception:
        return False
    return True


def _normalize_details(details: Mapping[str, object] | None) -> tuple[tuple[str, str], ...]:
    if not details:
        return ()

    normalized: list[tuple[str, str]] = []
    for key, value in details.items():
        safe_key = str(key).strip()
        safe_value = _redact_detail_value(safe_key, value)
        normalized.append((safe_key, safe_value))
    return tuple(normalized)


def _redact_detail_value(key: str, value: object) -> str:
    normalized_key = key.lower()
    if any(secret_key in normalized_key for secret_key in SENSITIVE_DETAIL_KEYS):
        return "[REDACTED]"
    return str(value)
