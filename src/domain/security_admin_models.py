from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SecurityPrincipal:
    principal_id: str
    username: str
    email: str
    display_name: str
    auth_source: str
    is_active: bool
    roles_count: int
    last_login_at: datetime | None
    updated_at: datetime | None


@dataclass(frozen=True)
class SecurityRole:
    role_id: str
    role_name: str
    description: str
    is_active: bool
    users_count: int
    permissions_count: int
    updated_at: datetime | None


@dataclass(frozen=True)
class SecurityPermission:
    permission_id: str
    permission_name: str
    description: str
    is_active: bool
    role_count: int


@dataclass(frozen=True)
class SecurityAuditRecord:
    event_id: str
    event_type: str
    principal_id: str
    resource: str
    action: str
    result: str
    occurred_at: datetime | None
    details_summary: str
    correlation_id: str


@dataclass(frozen=True)
class SecurityPrincipalDetail:
    principal: SecurityPrincipal
    assigned_roles: tuple[str, ...]
    effective_permissions: tuple[str, ...]
    related_audit: tuple[SecurityAuditRecord, ...]


@dataclass(frozen=True)
class SecurityRoleDetail:
    role: SecurityRole
    assigned_permissions: tuple[str, ...]
    assigned_principals: tuple[str, ...]

