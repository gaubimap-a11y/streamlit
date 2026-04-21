from __future__ import annotations

from importlib import import_module
from typing import Any

_AUTH_SERVICE_NAMES = {
    "AuthService",
    "REASON_ACCOUNT_LOCKED",
    "REASON_WRONG_CREDENTIALS",
    "AuthorizationService",
}
_SSO_SERVICE_NAMES = {
    "AUTH_SOURCE_INTERNAL",
    "AUTH_SOURCE_SSO",
    "ABSOLUTE_SESSION_HOURS",
    "AuthUserRecord",
    "AuthUserStore",
    "AuthenticatedSession",
    "AuthorizationStore",
    "DEFAULT_SSO_BASIC_PERMISSIONS",
    "InactiveUserError",
    "InvalidCredentialsError",
    "LoginRequest",
    "PermissionResolver",
    "SessionExpiredError",
    "SsoLoginRequest",
    "authenticate_sso_user",
    "authenticate_user",
    "build_authenticated_session",
    "ensure_session_is_active",
    "resolve_sso_identity_fields",
}

__all__ = sorted(_AUTH_SERVICE_NAMES | _SSO_SERVICE_NAMES)


def __getattr__(name: str) -> Any:
    if name in _AUTH_SERVICE_NAMES:
        module = import_module("src.application.auth.auth_service")
    elif name in _SSO_SERVICE_NAMES:
        module = import_module("src.application.auth.sso_auth_service")
    else:
        raise AttributeError(f"module 'src.application.auth' has no attribute {name!r}")
    try:
        return getattr(module, name)
    except AttributeError as exc:
        raise AttributeError(f"module 'src.application.auth' has no attribute {name!r}") from exc
