from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from threading import Thread
from typing import Protocol

from src.core.config import get_settings
from src.core.exceptions import AuthError, DataAccessError
from src.domain.auth_models import AUTH_SOURCE_INTERNAL, AuthenticatedSession, AuthUserRecord, LoginRequest
from src.domain.auth_validation import (
    AuthSystemUnavailableError,
    InactiveUserError,
    InvalidCredentialsError,
    validate_login_request,
)
from src.infrastructure.databricks.client import databricks_connection
from src.infrastructure.repositories.auth_user_store import DatabricksAuthUserStore
from src.infrastructure.repositories.authorization_store import DatabricksAuthorizationStore
from src.infrastructure.repositories.user_repository import UserRepository
from werkzeug.security import check_password_hash


REASON_ACCOUNT_LOCKED = "account_locked"
REASON_WRONG_CREDENTIALS = "wrong_credentials"
_LOGGER = logging.getLogger(__name__)


class AuthUserStore(Protocol):
    def get_user_by_username(self, username: str) -> AuthUserRecord | None: ...

    def update_last_login(self, user_id: str, logged_in_at: datetime) -> None: ...

    def verify_password(self, plain_password: str, password_hash: str) -> bool: ...


class AuthorizationStore(Protocol):
    def resolve_permissions(
        self,
        *,
        principal_id: str,
        username: str,
        email: str,
        auth_source: str,
    ) -> tuple[str, ...]: ...


class AuthService:
    def __init__(
        self,
        auth_user_store: AuthUserStore | None = None,
        authorization_store: AuthorizationStore | None = None,
        user_repository: UserRepository | None = None,
    ) -> None:
        self._auth_user_store = auth_user_store
        self._authorization_store = authorization_store
        self._legacy_user_repository = user_repository

    def authenticate_session(self, username: str, password: str, now: datetime | None = None) -> AuthenticatedSession:
        request = LoginRequest(username=username, password=password)
        validate_login_request(request)
        login_time = now or datetime.now(tz=timezone.utc)
        normalized_username = request.username.strip().lower()

        user_store = self._resolve_user_store()
        try:
            user = user_store.get_user_by_username(normalized_username)
        except (AuthSystemUnavailableError, DataAccessError, AuthError) as exc:
            _LOGGER.error("Authentication lookup failed for username=%s: %s", normalized_username, exc)
            raise AuthError("Authentication data source is unavailable.") from exc

        if user is None:
            raise InvalidCredentialsError("Invalid username or password.")
        if not user.is_active:
            try:
                password_matches = user_store.verify_password(request.password, user.password_hash)
            except (AuthSystemUnavailableError, DataAccessError, AuthError) as exc:
                _LOGGER.error(
                    "Password verification failed for inactive user username=%s: %s",
                    normalized_username,
                    exc,
                )
                raise AuthError("Authentication data source is unavailable.") from exc
            if password_matches:
                raise InactiveUserError("User account is inactive.")
            raise InvalidCredentialsError("Invalid username or password.")

        try:
            password_matches = user_store.verify_password(request.password, user.password_hash)
        except (AuthSystemUnavailableError, DataAccessError, AuthError) as exc:
            _LOGGER.error("Password verification failed for username=%s: %s", normalized_username, exc)
            raise AuthError("Authentication data source is unavailable.") from exc
        if not password_matches:
            raise InvalidCredentialsError("Invalid username or password.")

        session = AuthenticatedSession(
            user_id=user.user_id,
            username=user.username,
            login_at=login_time,
            expires_at=login_time + timedelta(hours=get_settings().session_timeout_hours),
            auth_source=AUTH_SOURCE_INTERNAL,
            display_name=user.display_name or user.username,
            email=user.email or user.username,
            permissions=self._resolve_permissions(user),
        )
        self._update_last_login_async(user_store, user.user_id, login_time)
        return session

    def authenticate_detail(self, username: str, password: str) -> tuple[bool, str | None]:
        try:
            self.authenticate_session(username, password)
        except InactiveUserError:
            return False, REASON_ACCOUNT_LOCKED
        except InvalidCredentialsError:
            return False, REASON_WRONG_CREDENTIALS
        return True, None

    def is_session_expired(
        self,
        login_time: datetime,
        now: datetime | None = None,
    ) -> bool:
        current_time = now or datetime.now(tz=timezone.utc)
        return current_time - login_time >= timedelta(
            hours=get_settings().session_timeout_hours,
        )

    def _resolve_user_store(self) -> AuthUserStore:
        if self._auth_user_store is not None:
            return self._auth_user_store
        if self._legacy_user_repository is not None:
            return _LegacyUserRepositoryAuthStore(self._legacy_user_repository)
        try:
            return DatabricksAuthUserStore.from_current_config()
        except Exception as exc:
            _LOGGER.error("Failed to initialize authentication user store: %s", exc)
            raise AuthError("Authentication data source is unavailable.") from exc

    def _resolve_permissions(self, user: AuthUserRecord) -> tuple[str, ...]:
        store = self._authorization_store
        if store is None:
            try:
                store = DatabricksAuthorizationStore.from_current_config()
            except Exception as exc:
                _LOGGER.error(
                    "Failed to initialize authorization store for user_id=%s: %s",
                    user.user_id,
                    exc,
                )
                raise AuthError("Authorization data source is unavailable.") from exc

        try:
            return tuple(
                store.resolve_permissions(
                    principal_id=user.user_id,
                    username=user.username,
                    email=user.email or user.username,
                    auth_source=AUTH_SOURCE_INTERNAL,
                )
            )
        except Exception as exc:
            _LOGGER.error(
                "Failed to resolve permissions for user_id=%s username=%s: %s",
                user.user_id,
                user.username,
                exc,
            )
            raise AuthError("Authorization data source is unavailable.") from exc

    def _update_last_login_async(self, user_store: AuthUserStore, user_id: str, login_time: datetime) -> None:
        def _worker() -> None:
            try:
                user_store.update_last_login(user_id, login_time)
            except Exception as exc:
                _LOGGER.error("Failed to update last_login asynchronously for user_id=%s: %s", user_id, exc)
                return

        Thread(target=_worker, name="auth-update-last-login", daemon=True).start()


class _LegacyUserRepositoryAuthStore:
    def __init__(self, user_repository: UserRepository) -> None:
        self._user_repository = user_repository

    def get_user_by_username(self, username: str) -> AuthUserRecord | None:
        try:
            with databricks_connection() as conn:
                row = self._user_repository.find_by_username(username, conn)
        except DataAccessError as exc:
            raise AuthError("Authentication data source is unavailable.") from exc

        if row is None:
            return None
        return AuthUserRecord(
            user_id=row.user_id,
            username=row.username,
            email=row.email,
            password_hash=row.password_hash,
            created_at=None,
            last_login_at=None,
            is_active=row.is_active,
            display_name=row.username,
        )

    def update_last_login(self, user_id: str, logged_in_at: datetime) -> None:
        return None

    def verify_password(self, plain_password: str, password_hash: str) -> bool:
        try:
            return check_password_hash(password_hash, plain_password)
        except ValueError as exc:
            raise AuthSystemUnavailableError("Stored password hash is not compatible with werkzeug.") from exc
