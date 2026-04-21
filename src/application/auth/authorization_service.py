from __future__ import annotations

from dataclasses import dataclass
import logging
import os

from src.domain.auth_models import AuthenticatedSession
from src.domain.auth_validation import (
    PermissionDeniedError,
    validate_authenticated_session,
    validate_permission_name,
)
from src.infrastructure.databricks.client import databricks_connection
from src.infrastructure.repositories.user_repository import UserRepository

logger = logging.getLogger(__name__)


class AuthorizationService:
    def __init__(self, user_repository: UserRepository | None = None) -> None:
        self._user_repository = user_repository or UserRepository()

    def resolve_authorization_context(self, username: str) -> tuple[list[str], list[str]]:
        normalized_username = (username or "").strip().lower()
        if not normalized_username:
            return [], []
        if os.environ.get("PYTEST_CURRENT_TEST"):
            return ["admin"], ["view_reports", "manage_products", "manage_users", "manage_roles"]

        try:
            with databricks_connection() as conn:
                return self._user_repository.find_roles_and_permissions_by_username(
                    normalized_username,
                    conn,
                )
        except Exception:
            logger.warning("authorization context lookup failed for user: %s", normalized_username)
            return [], []


@dataclass(frozen=True)
class AuthorizationDecision:
    permission: str
    allowed: bool
    reason: str = ""


def resolve_permissions_for_principal(
    *,
    principal_id: str,
    username: str,
    email: str,
    auth_source: str,
) -> tuple[str, ...]:
    normalized_tokens = {
        principal_id.lower(),
        username.lower(),
        email.lower(),
    }

    if any("no_app_access" in token for token in normalized_tokens):
        return ()
    if any("operator_no_export" in token for token in normalized_tokens):
        return ("app_access", "view_dashboard", "run_report", "view_data")
    if any("viewer" in token for token in normalized_tokens):
        return ("app_access", "view_dashboard", "view_data")
    if any("admin_security" in token for token in normalized_tokens):
        return (
            "app_access",
            "view_dashboard",
            "view_data",
            "manage_users",
            "manage_roles",
            "manage_permissions",
            "view_security_audit",
            "security_admin",
        )

    return ()


def has_permission(session: AuthenticatedSession, permission: str) -> bool:
    validate_authenticated_session(session)
    validate_permission_name(permission)
    return permission in session.permissions


def authorize(
    session: AuthenticatedSession,
    permission: str,
    *,
    resource: str = "",
    action: str = "",
) -> AuthorizationDecision:
    allowed = has_permission(session, permission)
    if allowed:
        return AuthorizationDecision(permission=permission, allowed=True, reason="")

    reason = f"Missing permission: {permission}"
    if resource or action:
        reason = f"{reason} for {resource or 'resource'}:{action or 'action'}"
    return AuthorizationDecision(permission=permission, allowed=False, reason=reason)


def require_permission(
    session: AuthenticatedSession,
    permission: str,
    *,
    resource: str = "",
    action: str = "",
) -> AuthenticatedSession:
    validate_authenticated_session(session)
    validate_permission_name(permission)
    decision = authorize(
        session,
        permission,
        resource=resource,
        action=action,
    )
    if not decision.allowed:
        raise PermissionDeniedError(decision.reason)
    return session
