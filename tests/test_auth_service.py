from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest

from src.application.auth.auth_service import (
    REASON_ACCOUNT_LOCKED,
    REASON_WRONG_CREDENTIALS,
    AuthService,
)
from src.core.exceptions import AuthError
from src.domain.auth_models import AUTH_SOURCE_INTERNAL, AuthUserRecord
from src.domain.auth_validation import InactiveUserError, InvalidCredentialsError


_BASE_TIME = datetime(2026, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
_PASSWORD_HASH = "$2b$12$exampleexampleexampleexampleexampleexampleexampleex"


def _active_user() -> AuthUserRecord:
    return AuthUserRecord(
        user_id="user-001",
        username="admin",
        email="admin@example.com",
        password_hash=_PASSWORD_HASH,
        created_at=None,
        last_login_at=None,
        is_active=True,
        display_name="Admin",
    )


def _inactive_user() -> AuthUserRecord:
    return AuthUserRecord(
        user_id="user-001",
        username="admin",
        email="admin@example.com",
        password_hash=_PASSWORD_HASH,
        created_at=None,
        last_login_at=None,
        is_active=False,
        display_name="Admin",
    )


def _build_service(
    mocker,
    *,
    user: AuthUserRecord | None,
    password_matches: bool = True,
    permissions: tuple[str, ...] = ("app_access", "view_dashboard"),
    authz_error: Exception | None = None,
) -> tuple[AuthService, object, object]:
    auth_user_store = mocker.MagicMock()
    auth_user_store.get_user_by_username.return_value = user
    auth_user_store.verify_password.return_value = password_matches

    authorization_store = mocker.MagicMock()
    if authz_error is None:
        authorization_store.resolve_permissions.return_value = permissions
    else:
        authorization_store.resolve_permissions.side_effect = authz_error

    service = AuthService(
        auth_user_store=auth_user_store,
        authorization_store=authorization_store,
    )
    return service, auth_user_store, authorization_store


def _mock_settings(mocker, timeout_hours: int = 8) -> None:
    class _Settings:
        pass

    _Settings.session_timeout_hours = timeout_hours

    mocker.patch("src.application.auth.auth_service.get_settings", return_value=_Settings())


def test_authenticate_session_returns_authenticated_session(mocker):
    _mock_settings(mocker)
    service, auth_user_store, authorization_store = _build_service(
        mocker,
        user=_active_user(),
        password_matches=True,
    )
    update_last_login = mocker.patch("src.application.auth.auth_service.AuthService._update_last_login_async")

    session = service.authenticate_session("admin", "password123", now=_BASE_TIME)

    assert session.user_id == "user-001"
    assert session.username == "admin"
    assert session.auth_source == AUTH_SOURCE_INTERNAL
    assert session.display_name == "Admin"
    assert session.email == "admin@example.com"
    assert session.permissions == ("app_access", "view_dashboard")
    assert session.login_at == _BASE_TIME
    assert session.expires_at == _BASE_TIME + timedelta(hours=8)
    assert session.has_permission("app_access") is True
    auth_user_store.get_user_by_username.assert_called_once_with("admin")
    auth_user_store.verify_password.assert_called_once_with("password123", _PASSWORD_HASH)
    authorization_store.resolve_permissions.assert_called_once_with(
        principal_id="user-001",
        username="admin",
        email="admin@example.com",
        auth_source=AUTH_SOURCE_INTERNAL,
    )
    update_last_login.assert_called_once_with(auth_user_store, "user-001", _BASE_TIME)


def test_authenticate_session_raises_inactive_user_error_when_password_matches(mocker):
    _mock_settings(mocker)
    service, auth_user_store, _authorization_store = _build_service(
        mocker,
        user=_inactive_user(),
        password_matches=True,
    )
    update_last_login = mocker.patch("src.application.auth.auth_service.AuthService._update_last_login_async")

    with pytest.raises(InactiveUserError):
        service.authenticate_session("admin", "password123", now=_BASE_TIME)

    auth_user_store.verify_password.assert_called_once_with("password123", _PASSWORD_HASH)
    update_last_login.assert_not_called()


def test_authenticate_session_raises_invalid_credentials_when_password_wrong(mocker):
    _mock_settings(mocker)
    service, auth_user_store, _authorization_store = _build_service(
        mocker,
        user=_active_user(),
        password_matches=False,
    )
    update_last_login = mocker.patch("src.application.auth.auth_service.AuthService._update_last_login_async")

    with pytest.raises(InvalidCredentialsError):
        service.authenticate_session("admin", "wrong_password", now=_BASE_TIME)

    auth_user_store.verify_password.assert_called_once_with("wrong_password", _PASSWORD_HASH)
    update_last_login.assert_not_called()


def test_authenticate_session_raises_invalid_credentials_when_user_not_found(mocker):
    _mock_settings(mocker)
    service, auth_user_store, _authorization_store = _build_service(
        mocker,
        user=None,
        password_matches=True,
    )
    update_last_login = mocker.patch("src.application.auth.auth_service.AuthService._update_last_login_async")

    with pytest.raises(InvalidCredentialsError):
        service.authenticate_session("ghost", "password123", now=_BASE_TIME)

    auth_user_store.verify_password.assert_not_called()
    update_last_login.assert_not_called()


def test_authenticate_session_fails_closed_when_permissions_lookup_unavailable(mocker, caplog):
    _mock_settings(mocker)
    service, auth_user_store, _authorization_store = _build_service(
        mocker,
        user=_active_user(),
        password_matches=True,
        authz_error=RuntimeError("databricks unavailable"),
    )
    update_last_login = mocker.patch("src.application.auth.auth_service.AuthService._update_last_login_async")

    with caplog.at_level(logging.ERROR, logger="src.application.auth.auth_service"), pytest.raises(AuthError):
        service.authenticate_session("admin", "password123", now=_BASE_TIME)

    auth_user_store.verify_password.assert_called_once_with("password123", _PASSWORD_HASH)
    update_last_login.assert_not_called()
    assert "Failed to resolve permissions" in caplog.text


def test_authenticate_session_raises_auth_error_when_user_store_unavailable(mocker, caplog):
    _mock_settings(mocker)
    mocker.patch(
        "src.application.auth.auth_service.DatabricksAuthUserStore.from_current_config",
        side_effect=RuntimeError("missing databricks config"),
    )
    service = AuthService()

    with caplog.at_level(logging.ERROR, logger="src.application.auth.auth_service"), pytest.raises(AuthError):
        service.authenticate_session("admin", "password123", now=_BASE_TIME)

    assert "Failed to initialize authentication user store:" in caplog.text


def test_authenticate_detail_returns_none_reason_on_success(mocker):
    _mock_settings(mocker)
    service, _auth_user_store, _authorization_store = _build_service(
        mocker,
        user=_active_user(),
        password_matches=True,
    )
    mocker.patch("src.application.auth.auth_service.AuthService._update_last_login_async")

    result, reason = service.authenticate_detail("admin", "password123")

    assert result is True
    assert reason is None


def test_authenticate_detail_maps_wrong_credentials_reason(mocker):
    _mock_settings(mocker)
    service, _auth_user_store, _authorization_store = _build_service(
        mocker,
        user=_active_user(),
        password_matches=False,
    )
    mocker.patch("src.application.auth.auth_service.AuthService._update_last_login_async")

    result, reason = service.authenticate_detail("admin", "wrong_password")

    assert result is False
    assert reason == REASON_WRONG_CREDENTIALS


def test_authenticate_detail_maps_locked_reason(mocker):
    _mock_settings(mocker)
    service, _auth_user_store, _authorization_store = _build_service(
        mocker,
        user=_inactive_user(),
        password_matches=True,
    )
    mocker.patch("src.application.auth.auth_service.AuthService._update_last_login_async")

    result, reason = service.authenticate_detail("admin", "password123")

    assert result is False
    assert reason == REASON_ACCOUNT_LOCKED

def test_is_session_expired_returns_false_before_8_hours(mocker):
    class _S:
        session_timeout_hours = 8

    mocker.patch("src.application.auth.auth_service.get_settings", return_value=_S())

    now = _BASE_TIME + timedelta(hours=7, minutes=59)
    assert AuthService().is_session_expired(_BASE_TIME, now=now) is False


def test_is_session_expired_returns_true_at_8_hours_or_more(mocker):
    class _S:
        session_timeout_hours = 8

    mocker.patch("src.application.auth.auth_service.get_settings", return_value=_S())

    assert AuthService().is_session_expired(_BASE_TIME, now=_BASE_TIME + timedelta(hours=8))
    assert AuthService().is_session_expired(_BASE_TIME, now=_BASE_TIME + timedelta(hours=8, minutes=1))
