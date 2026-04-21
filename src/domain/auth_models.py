from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

AUTH_SOURCE_INTERNAL = "internal"
AUTH_SOURCE_SSO = "sso"

SSO_PROVIDER_GOOGLE = "google"

DEFAULT_PERMISSIONS = (
    "app_access",
    "view_dashboard",
    "run_report",
    "view_data",
    "export_output",
    "manage_users",
    "manage_roles",
    "manage_permissions",
    "view_security_audit",
    "security_admin",
)

ALL_PERMISSIONS = DEFAULT_PERMISSIONS


@dataclass(frozen=True)
class LoginRequest:
    username: str
    password: str


@dataclass(frozen=True)
class AuthUserRecord:
    user_id: str
    username: str
    email: str
    password_hash: str
    created_at: datetime | None
    last_login_at: datetime | None
    is_active: bool
    display_name: str = ""


@dataclass(frozen=True)
class SsoLoginRequest:
    principal_id: str
    email: str
    display_name: str
    provider_id: str = SSO_PROVIDER_GOOGLE
    claims: tuple[tuple[str, str], ...] = ()
    correlation_id: str = ""


@dataclass(frozen=True)
class AuthenticatedSession:
    user_id: str
    username: str
    login_at: datetime
    expires_at: datetime
    auth_source: str = AUTH_SOURCE_INTERNAL
    display_name: str = ""
    email: str = ""
    permissions: tuple[str, ...] = ()
    correlation_id: str = ""

    @property
    def principal_id(self) -> str:
        return self.user_id

    def has_permission(self, permission: str) -> bool:
        return permission in self.permissions
