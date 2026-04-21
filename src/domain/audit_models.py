from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class AuditEvent:
    event_id: str
    event_type: str
    occurred_at: datetime
    principal_id: str
    auth_source: str
    resource: str
    action: str
    result: str
    correlation_id: str = ""
    details: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def as_record(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "occurred_at": self.occurred_at.isoformat(),
            "principal_id": self.principal_id,
            "auth_source": self.auth_source,
            "resource": self.resource,
            "action": self.action,
            "result": self.result,
            "correlation_id": self.correlation_id,
            "details_json": json.dumps(dict(self.details), sort_keys=True, separators=(",", ":")),
        }


def create_audit_event_id() -> str:
    return uuid4().hex
