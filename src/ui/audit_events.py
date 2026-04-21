from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from src.application.auth.audit_service import AuditEventWriter, build_audit_event_from_session, record_audit_event
from src.domain.auth_models import AuthenticatedSession
from src.infrastructure.repositories.audit_event_store import DatabricksAuditEventStore


_AUDIT_WRITER_SENTINEL = object()
_AUDIT_WRITER: AuditEventWriter | None | object = _AUDIT_WRITER_SENTINEL


def get_audit_writer() -> AuditEventWriter | None:
    global _AUDIT_WRITER
    if _AUDIT_WRITER is _AUDIT_WRITER_SENTINEL:
        try:
            _AUDIT_WRITER = DatabricksAuditEventStore.from_current_config()
        except Exception:
            _AUDIT_WRITER = None
    return None if _AUDIT_WRITER is _AUDIT_WRITER_SENTINEL else _AUDIT_WRITER


def record_ui_audit_event(
    session: AuthenticatedSession | None,
    *,
    event_type: str,
    resource: str,
    action: str,
    result: str,
    details: Mapping[str, object] | None = None,
    occurred_at: datetime | None = None,
) -> bool:
    writer = get_audit_writer()
    event = build_audit_event_from_session(
        session,
        event_type=event_type,
        resource=resource,
        action=action,
        result=result,
        occurred_at=occurred_at,
        details=details,
    )
    return record_audit_event(writer, event)
