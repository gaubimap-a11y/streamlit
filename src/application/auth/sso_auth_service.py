from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Thread
from typing import Protocol

from src.domain.auth_models import (
    AUTH_SOURCE_INTERNAL,
    AUTH_SOURCE_SSO,
    AuthenticatedSession,
    AuthUserRecord,
    LoginRequest,
    SsoLoginRequest,
)
from src.domain.auth_validation import (
    AuthenticationValidationError,
    AuthSystemUnavailableError,
    InactiveUserError,
    InvalidCredentialsError,
    SessionExpiredError,
    validate_authenticated_session,
    validate_login_request,
)
ABSOLUTE_SESSION_HOURS = 8


class AuthUserStore(Protocol):
    def get_user_by_username(self, username: str) -> AuthUserRecord | None: ...

    def update_last_login(self, user_id: str, logged_in_at: datetime) -> None: ...

    def verify_password(self, plain_password: str, password_hash: str) -> bool: ...


class PermissionResolver(Protocol):
    def resolve_permissions(
        self,
        *,
        principal_id: str,
        username: str,
        email: str,
        auth_source: str,
    ) -> tuple[str, ...]: ...


class AuthorizationStore(Protocol):
    def has_principal_mapping(
        self,
        *,
        principal_id: str,
        username: str,
        email: str,
        auth_source: str,
    ) -> bool: ...

    def resolve_permissions(
        self,
        *,
        principal_id: str,
        username: str,
        email: str,
        auth_source: str,
    ) -> tuple[str, ...]: ...


DEFAULT_SSO_BASIC_PERMISSIONS = ("app_access", "view_dashboard")


def authenticate_user(
    request: LoginRequest,
    user_store: AuthUserStore,
    now: datetime | None = None,
    authorization_store: AuthorizationStore | None = None,
) -> AuthenticatedSession:
    validate_login_request(request)
    login_time = now or datetime.now(tz=timezone.utc)

    try:
        user = user_store.get_user_by_username(request.username)
    except AuthSystemUnavailableError:
        raise

    if user is None:
        raise InvalidCredentialsError("Invalid username or password.")
    if not user.is_active:
        raise InactiveUserError("User account is inactive.")
    if not user_store.verify_password(request.password, user.password_hash):
        raise InvalidCredentialsError("Invalid username or password.")

    session = build_authenticated_session(
        user,
        login_time,
        auth_source=AUTH_SOURCE_INTERNAL,
        permissions=_resolve_permissions(
            authorization_store,
            principal_id=user.user_id,
            username=user.username,
            email=user.email,
            auth_source=AUTH_SOURCE_INTERNAL,
        ),
    )
    _update_last_login_async(user_store, user.user_id, login_time)
    return session


def authenticate_sso_user(
    request: SsoLoginRequest,
    now: datetime | None = None,
    authorization_store: AuthorizationStore | None = None,
    *,
    principal_mapped: bool | None = None,
) -> AuthenticatedSession:
    principal_id, email, display_name = resolve_sso_identity_fields(request)

    login_time = now or datetime.now(tz=timezone.utc)

    store = authorization_store
    if store is None:
        try:
            from src.infrastructure.repositories.authorization_store import DatabricksAuthorizationStore

            store = DatabricksAuthorizationStore.from_current_config()
        except Exception:
            store = None

    mapped = principal_mapped
    if mapped is None and store is not None:
        try:
            mapped = store.has_principal_mapping(
                principal_id=principal_id,
                username=email,
                email=email,
                auth_source=AUTH_SOURCE_SSO,
            )
        except Exception:
            mapped = None

    if mapped is False:
        _maybe_upsert_basic_sso_principal(
            store,
            principal_id=principal_id,
            username=email,
            email=email,
            display_name=display_name,
        )
    return build_authenticated_session(
        AuthUserRecord(
            user_id=principal_id,
            username=email,
            email=email,
            password_hash="",
            created_at=None,
            last_login_at=None,
            is_active=True,
            display_name=display_name,
        ),
        login_time,
        auth_source=AUTH_SOURCE_SSO,
        display_name=display_name,
        email=email,
        permissions=_resolve_sso_permissions(
            store,
            principal_id=principal_id,
            username=email,
            email=email,
            principal_mapped=mapped,
        ),
    )


def resolve_sso_identity_fields(request: SsoLoginRequest) -> tuple[str, str, str]:
    claims = _normalize_claims(request.claims)
    principal_id = _resolve_sso_principal_id(request, claims)
    email = _resolve_sso_email(request, claims)
    display_name = _resolve_sso_display_name(request, claims, email=email, principal_id=principal_id)

    if principal_id == "":
        raise AuthenticationValidationError("principal_id is required.")
    if email == "":
        raise AuthenticationValidationError("email is required.")
    if display_name == "":
        raise AuthenticationValidationError("display_name is required.")
    return principal_id, email, display_name


def build_authenticated_session(
    user: AuthUserRecord,
    login_time: datetime,
    *,
    auth_source: str = AUTH_SOURCE_INTERNAL,
    display_name: str = "",
    email: str = "",
    permissions: tuple[str, ...] = (),
    correlation_id: str = "",
) -> AuthenticatedSession:
    return AuthenticatedSession(
        user_id=user.user_id,
        username=user.username,
        login_at=login_time,
        expires_at=login_time + timedelta(hours=ABSOLUTE_SESSION_HOURS),
        auth_source=auth_source,
        display_name=display_name or user.display_name or user.username,
        email=email or user.email,
        permissions=permissions,
        correlation_id=correlation_id,
    )


def ensure_session_is_active(
    session: AuthenticatedSession | None,
    now: datetime | None = None,
) -> AuthenticatedSession:
    if session is None:
        raise SessionExpiredError("Session is not authenticated.")

    current_time = now or datetime.now(tz=timezone.utc)
    if current_time >= session.expires_at:
        raise SessionExpiredError("Session has expired.")
    validate_authenticated_session(session)
    return session


def _update_last_login_async(user_store: AuthUserStore, user_id: str, login_time: datetime) -> None:
    def _worker() -> None:
        try:
            user_store.update_last_login(user_id, login_time)
        except AuthSystemUnavailableError:
            return

    Thread(target=_worker, name="auth-update-last-login", daemon=True).start()


def _resolve_permissions(
    authorization_store: AuthorizationStore | None,
    *,
    principal_id: str,
    username: str,
    email: str,
    auth_source: str,
) -> tuple[str, ...]:
    store = authorization_store
    if store is None:
        try:
            from src.infrastructure.repositories.authorization_store import DatabricksAuthorizationStore

            store = DatabricksAuthorizationStore.from_current_config()
        except Exception:
            return ()

    try:
        return tuple(
            store.resolve_permissions(
                principal_id=principal_id,
                username=username,
                email=email,
                auth_source=auth_source,
            )
        )
    except Exception:
        return ()


def _resolve_sso_permissions(
    authorization_store: AuthorizationStore | None,
    *,
    principal_id: str,
    username: str,
    email: str,
    principal_mapped: bool | None = None,
) -> tuple[str, ...]:
    store = authorization_store
    if store is None:
        try:
            from src.infrastructure.repositories.authorization_store import DatabricksAuthorizationStore

            store = DatabricksAuthorizationStore.from_current_config()
        except Exception:
            return ()

    if principal_mapped is None:
        try:
            mapped = store.has_principal_mapping(
                principal_id=principal_id,
                username=username,
                email=email,
                auth_source=AUTH_SOURCE_SSO,
            )
        except Exception:
            return ()
    else:
        mapped = principal_mapped

    if not mapped:
        return DEFAULT_SSO_BASIC_PERMISSIONS

    try:
        permissions = tuple(
            store.resolve_permissions(
                principal_id=principal_id,
                username=username,
                email=email,
                auth_source=AUTH_SOURCE_SSO,
            )
        )
    except Exception:
        return ()

    return permissions


def _maybe_upsert_basic_sso_principal(
    authorization_store: AuthorizationStore | None,
    *,
    principal_id: str,
    username: str,
    email: str,
    display_name: str,
) -> None:
    if authorization_store is None:
        return

    upsert = getattr(authorization_store, "upsert_basic_sso_principal", None)
    if not callable(upsert):
        return

    try:
        upsert(
            principal_id=principal_id,
            username=username,
            email=email,
            display_name=display_name,
            password_hash="",
        )
    except Exception:
        return


def _normalize_claims(claims: tuple[tuple[str, str], ...]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in claims:
        normalized_key = str(key).strip().lower()
        normalized_value = str(value).strip()
        if normalized_key and normalized_value:
            normalized[normalized_key] = normalized_value
    return normalized


def _resolve_sso_principal_id(request: SsoLoginRequest, claims: dict[str, str]) -> str:
    direct_value = _normalize_optional_string(request.principal_id)
    if direct_value:
        return direct_value
    subject = claims.get("sub", "").strip()
    if subject == "":
        for claim_name in ("principal_id", "email"):
            candidate = claims.get(claim_name, "").strip()
            if candidate:
                return candidate
        return ""

    issuer = claims.get("iss", "").strip()
    if issuer:
        return f"{issuer}|{subject}"
    return subject


def _resolve_sso_email(request: SsoLoginRequest, claims: dict[str, str]) -> str:
    direct_value = _normalize_optional_string(request.email)
    if direct_value:
        return direct_value
    for claim_name in ("email", "preferred_username", "upn", "username", "login"):
        candidate = claims.get(claim_name, "").strip()
        if candidate:
            return candidate
    return ""


def _resolve_sso_display_name(
    request: SsoLoginRequest,
    claims: dict[str, str],
    *,
    email: str,
    principal_id: str,
) -> str:
    direct_value = _normalize_optional_string(request.display_name)
    if direct_value:
        return direct_value
    for claim_name in ("name", "display_name"):
        candidate = claims.get(claim_name, "").strip()
        if candidate:
            return candidate
    if email:
        return email
    return principal_id


def _normalize_optional_string(value: str) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()
