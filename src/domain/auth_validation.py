from __future__ import annotations

from datetime import datetime

from src.domain.auth_models import AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SSO, AuthenticatedSession, LoginRequest


class AuthenticationError(RuntimeError):
    """Base class for authentication-related failures."""


class AuthenticationValidationError(ValueError):
    """Raised when login input does not meet the authentication contract."""


class InvalidCredentialsError(AuthenticationError):
    """Raised when login credentials do not match any active account."""


class InactiveUserError(AuthenticationError):
    """Raised when a matched account is inactive."""


class AuthSystemUnavailableError(AuthenticationError):
    """Raised when the authentication system is unavailable."""


class SessionExpiredError(AuthenticationError):
    """Raised when an authenticated session has expired."""


class PermissionDeniedError(AuthenticationError):
    """Raised when a principal is missing a required permission."""


class AuditWriteError(RuntimeError):
    """Raised when an audit write cannot be completed safely."""


def validate_login_request(request: LoginRequest) -> None:
    if not isinstance(request.username, str):
        raise AuthenticationValidationError("username must be a string.")
    if not isinstance(request.password, str):
        raise AuthenticationValidationError("password must be a string.")

    if request.username.strip() == "":
        raise AuthenticationValidationError("username is required.")
    if request.password.strip() == "":
        raise AuthenticationValidationError("password is required.")

    if len(request.username) > 200:
        raise AuthenticationValidationError("username must be 200 characters or fewer.")
    if len(request.password) > 200:
        raise AuthenticationValidationError("password must be 200 characters or fewer.")

    for char in request.username:
        if ord(char) < 32 and char not in {"\t", "\n", "\r"}:
            raise AuthenticationValidationError("username contains unsupported control characters.")


def validate_auth_source(auth_source: str) -> None:
    if not isinstance(auth_source, str):
        raise AuthenticationValidationError("auth_source must be a string.")

    normalized = auth_source.strip()
    if normalized not in {AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SSO}:
        raise AuthenticationValidationError("auth_source must be internal or sso.")


def validate_authenticated_session(session: AuthenticatedSession) -> None:
    if not isinstance(session.user_id, str) or session.user_id.strip() == "":
        raise AuthenticationValidationError("session user_id is required.")
    if not isinstance(session.username, str) or session.username.strip() == "":
        raise AuthenticationValidationError("session username is required.")
    if not isinstance(session.login_at, datetime) or not isinstance(session.expires_at, datetime):
        raise AuthenticationValidationError("session timestamps are required.")

    validate_auth_source(session.auth_source)
    if session.login_at >= session.expires_at:
        raise AuthenticationValidationError("session expires_at must be after login_at.")

    for permission in session.permissions:
        validate_permission_name(permission)


def validate_permission_name(permission: str) -> None:
    if not isinstance(permission, str):
        raise AuthenticationValidationError("permission must be a string.")
    if permission.strip() == "":
        raise AuthenticationValidationError("permission is required.")
    for char in permission:
        if ord(char) < 32 and char not in {"\t", "\n", "\r"}:
            raise AuthenticationValidationError("permission contains unsupported control characters.")
